/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Architecture:
 *   Supervisor mode: long-running process that owns containers, a UNIX domain
 *   socket (control channel), a bounded-buffer log pipeline, and signal handling.
 *
 *   Client mode: short-lived; connects to supervisor socket, sends one
 *   control_request_t, reads one control_response_t, exits.
 *
 *   IPC paths:
 *     Path A (logging):  container stdout/stderr -> pipe -> producer thread
 *                        -> bounded_buffer_t -> consumer thread -> log file
 *     Path B (control):  CLI client -> UNIX domain socket -> supervisor event loop
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

/* ================================================================
 * Constants
 * ================================================================ */

#define STACK_SIZE          (1024 * 1024)
#define CONTAINER_ID_LEN    32
#define CONTROL_PATH        "/tmp/mini_runtime.sock"
#define LOG_DIR             "logs"
#define CONTROL_MESSAGE_LEN 512
#define CHILD_COMMAND_LEN   256
#define LOG_CHUNK_SIZE      4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT  (40UL << 20)
#define DEFAULT_HARD_LIMIT  (64UL << 20)

/* ================================================================
 * Types
 * ================================================================ */

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,   /* stopped via explicit stop command */
    CONTAINER_KILLED,    /* killed by hard memory limit */
    CONTAINER_EXITED     /* exited on its own */
} container_state_t;

typedef struct container_record {
    char              id[CONTAINER_ID_LEN];
    pid_t             host_pid;
    time_t            started_at;
    container_state_t state;
    unsigned long     soft_limit_bytes;
    unsigned long     hard_limit_bytes;
    int               exit_code;
    int               exit_signal;
    char              log_path[PATH_MAX];
    /* Task 4 attribution */
    int               stop_requested;
    /* logging pipeline */
    pthread_t         producer_thread;
    /* run-command wait */
    int               run_client_fd;
    struct container_record *next;
} container_record_t;

typedef struct {
    char   container_id[CONTAINER_ID_LEN];
    size_t length;
    char   data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t      items[LOG_BUFFER_CAPACITY];
    size_t          head;
    size_t          tail;
    size_t          count;
    int             shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char           container_id[CONTAINER_ID_LEN];
    char           rootfs[PATH_MAX];
    char           command[CHILD_COMMAND_LEN];
    unsigned long  soft_limit_bytes;
    unsigned long  hard_limit_bytes;
    int            nice_value;
} control_request_t;

typedef struct {
    int  status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int  nice_value;
    int  log_write_fd;
} child_config_t;

/* Combines clone stack + child config in one allocation so COW semantics
 * guarantee the child can safely read cfg after clone() returns in parent. */
typedef struct {
    char         stack[STACK_SIZE];
    child_config_t cfg;
} spawn_area_t;

typedef struct {
    int              server_fd;
    int              monitor_fd;
    int              should_stop;
    pthread_t        logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t  metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    int   pipe_read_fd;
    char  container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *log_buffer;
} producer_arg_t;

/* ================================================================
 * Globals (supervisor only)
 * ================================================================ */

static int g_signal_pipe[2] = {-1, -1};

/* Used by run-client signal handler to send CMD_STOP */
static char                  g_run_id[CONTAINER_ID_LEN];
static volatile sig_atomic_t g_run_stop_requested = 0;

/* ================================================================
 * CLI parsing helpers
 * ================================================================ */

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command>"
            " [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run   <id> <container-rootfs> <command>"
            " [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char         *end = NULL;
    unsigned long mib;

    errno = 0;
    mib   = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc, char *argv[],
                                int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long  nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1],
                               &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1],
                               &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno      = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr,
                "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ================================================================
 * Bounded buffer  (mutex + two condition variables)
 *
 * Race without synchronization: concurrent push/pop could corrupt
 * head/tail/count and overwrite live data.  A mutex ensures only
 * one thread modifies the ring at a time.  Condition variables
 * replace busy-waiting: producers sleep on not_full, consumers
 * sleep on not_empty.  shutdown broadcasts wake everyone so threads
 * can drain and exit rather than blocking forever.
 * ================================================================ */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));
    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }
    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * bounded_buffer_push - producer inserts one log chunk.
 *
 * Blocks while buffer is full (backpressure keeps producers from running
 * ahead infinitely).  Returns 0 on success, -1 if shutting down.
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * bounded_buffer_pop - consumer removes one log chunk.
 *
 * Blocks while buffer is empty.  Returns 0 and fills *item on success.
 * Returns -1 when shutting_down is set AND buffer is empty (drain complete).
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0) {
        /* shutdown + empty: nothing left to drain */
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ================================================================
 * Logging consumer thread
 *
 * Drains the bounded buffer and writes each chunk to the matching
 * per-container log file (append mode).  Keeps running after
 * shutdown begins until the buffer is fully drained so no log
 * lines are lost when a container exits abruptly.
 * ================================================================ */
void *logging_thread(void *arg)
{
    bounded_buffer_t *buf = (bounded_buffer_t *)arg;
    log_item_t        item;

    while (bounded_buffer_pop(buf, &item) == 0) {
        char path[PATH_MAX];
        int  fd;

        snprintf(path, sizeof(path), "%s/%.31s.log", LOG_DIR,
                 item.container_id);
        fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("logging_thread: open");
            continue;
        }
        /* write is best-effort; partial writes are acceptable for logs */
        (void)write(fd, item.data, item.length);
        close(fd);
    }
    return NULL;
}

/* ================================================================
 * Producer thread
 *
 * One per container.  Reads raw bytes from the pipe connected to
 * the container's stdout/stderr and pushes chunks into the shared
 * bounded buffer.  Exits on pipe EOF (write end closed when the
 * container process exits).
 * ================================================================ */
static void *producer_thread_fn(void *arg)
{
    producer_arg_t *p = (producer_arg_t *)arg;
    log_item_t      item;
    ssize_t         n;

    while (1) {
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, p->container_id, CONTAINER_ID_LEN - 1);

        n = read(p->pipe_read_fd, item.data, LOG_CHUNK_SIZE);
        if (n <= 0)
            break; /* EOF: container exited */

        item.length = (size_t)n;
        /* push blocks if buffer full; returns -1 only on shutdown */
        if (bounded_buffer_push(p->log_buffer, &item) != 0)
            break;
    }

    close(p->pipe_read_fd);
    free(p);
    return NULL;
}

/* ================================================================
 * Container child entrypoint  (runs after clone())
 * ================================================================ */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;
    char            cmd_copy[CHILD_COMMAND_LEN];
    char           *args[64];
    int             nargs = 0;
    char           *token;
    int             devnull;

    /* Private mount namespace: prevents our mounts from propagating
     * back to the host (especially /proc). */
    mount(NULL, "/", NULL, MS_PRIVATE | MS_REC, NULL);

    /* UTS namespace: give the container its own hostname */
    sethostname(cfg->id, strlen(cfg->id));

    /* Filesystem isolation: chroot into the container's rootfs */
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    /* Mount /proc so ps, top, etc. work inside the container */
    mount("proc", "/proc", "proc", 0, NULL);

    /* Redirect stdout/stderr to the supervisor logging pipe */
    dup2(cfg->log_write_fd, STDOUT_FILENO);
    dup2(cfg->log_write_fd, STDERR_FILENO);
    if (cfg->log_write_fd > STDERR_FILENO)
        close(cfg->log_write_fd);

    /* Redirect stdin from /dev/null (background container) */
    devnull = open("/dev/null", O_RDONLY);
    if (devnull >= 0) {
        dup2(devnull, STDIN_FILENO);
        if (devnull > STDIN_FILENO)
            close(devnull);
    }

    /* Apply scheduling priority */
    if (cfg->nice_value != 0)
        setpriority(PRIO_PROCESS, 0, cfg->nice_value);

    /* Parse space-separated command into argv */
    strncpy(cmd_copy, cfg->command, sizeof(cmd_copy) - 1);
    cmd_copy[sizeof(cmd_copy) - 1] = '\0';
    token = strtok(cmd_copy, " ");
    while (token && nargs < 63) {
        args[nargs++] = token;
        token = strtok(NULL, " ");
    }
    args[nargs] = NULL;

    if (nargs == 0) {
        write(STDERR_FILENO, "child_fn: empty command\n", 24);
        return 1;
    }

    execv(args[0], args);
    perror("execv");
    return 1;
}

/* ================================================================
 * Kernel monitor IPC helpers
 * ================================================================ */

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid              = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd,
                            const char *container_id,
                            pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

/* ================================================================
 * Supervisor internals
 * ================================================================ */

static container_record_t *find_container(supervisor_ctx_t *ctx,
                                          const char *id)
{
    container_record_t *c;
    for (c = ctx->containers; c; c = c->next)
        if (strcmp(c->id, id) == 0)
            return c;
    return NULL;
}

/* Signal handler: write signal number into self-pipe so the select()
 * loop can react without async-signal-safety issues. */
static void supervisor_signal_handler(int signo)
{
    unsigned char sig  = (unsigned char)signo;
    int           save = errno;
    (void)write(g_signal_pipe[1], &sig, 1);
    errno = save;
}

/* Launch a new container process and wire up its logging pipeline.
 *
 * Steps:
 *   1. Create pipe (supervisor read end, container write end).
 *   2. Allocate spawn_area (stack + child config) – single COW-safe allocation.
 *   3. clone() with PID / UTS / mount namespaces.
 *   4. Close write end in parent; start producer thread on read end.
 *   5. Register with kernel monitor.
 *   6. Insert container record into linked list.
 */
static int launch_container(supervisor_ctx_t *ctx,
                            const control_request_t *req)
{
    spawn_area_t       *sa;
    int                 pipefd[2];
    pid_t               pid;
    container_record_t *rec;
    producer_arg_t     *parg;

    sa = malloc(sizeof(*sa));
    if (!sa)
        return -1;

    if (pipe(pipefd) != 0) {
        free(sa);
        return -1;
    }
    /* Read end is only needed in parent; mark close-on-exec so
     * future clone() calls don't leak it into other containers. */
    fcntl(pipefd[0], F_SETFD, FD_CLOEXEC);

    /* Prepare child configuration inside the spawn area so the child
     * reads its own COW copy after clone(). */
    memset(&sa->cfg, 0, sizeof(sa->cfg));
    strncpy(sa->cfg.id,       req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(sa->cfg.rootfs,   req->rootfs,        PATH_MAX - 1);
    strncpy(sa->cfg.command,  req->command,        CHILD_COMMAND_LEN - 1);
    sa->cfg.nice_value    = req->nice_value;
    sa->cfg.log_write_fd  = pipefd[1];

    pid = clone(child_fn,
                sa->stack + STACK_SIZE,
                CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                &sa->cfg);

    /* Parent closes the write end immediately; EOF reaches the producer
     * thread only when the container process closes its copy (on exit). */
    close(pipefd[1]);

    if (pid < 0) {
        perror("clone");
        close(pipefd[0]);
        free(sa);
        return -1;
    }

    /* sa->stack was the child's runtime stack; child has its own COW copy,
     * so parent can free safely. */
    free(sa);

    /* Allocate and populate container record */
    rec = calloc(1, sizeof(*rec));
    if (!rec) {
        kill(pid, SIGKILL);
        close(pipefd[0]);
        return -1;
    }
    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    rec->host_pid         = pid;
    rec->started_at       = time(NULL);
    rec->state            = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->exit_code        = 0;
    rec->exit_signal      = 0;
    rec->stop_requested   = 0;
    rec->run_client_fd    = -1;
    snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, req->container_id);

    /* Register with kernel monitor (optional – module may not be loaded) */
    if (ctx->monitor_fd >= 0)
        register_with_monitor(ctx->monitor_fd, req->container_id, pid,
                              req->soft_limit_bytes, req->hard_limit_bytes);

    /* Start producer thread – it owns pipefd[0] and frees parg on exit */
    parg = malloc(sizeof(*parg));
    if (parg) {
        parg->pipe_read_fd = pipefd[0];
        strncpy(parg->container_id, req->container_id, CONTAINER_ID_LEN - 1);
        parg->log_buffer = &ctx->log_buffer;
        pthread_create(&rec->producer_thread, NULL, producer_thread_fn, parg);
    } else {
        /* Fallback: nobody reads the pipe; close it to avoid fd leak */
        close(pipefd[0]);
    }

    /* Insert at head of linked list under metadata lock */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next         = ctx->containers;
    ctx->containers   = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    return 0;
}

/* Reap all exited children, update metadata, notify waiting run clients.
 * Producer thread is joined here (it exits on pipe EOF which happens
 * when the container process terminates). */
static void reap_children(supervisor_ctx_t *ctx)
{
    int   wstatus;
    pid_t pid;

    while ((pid = waitpid(-1, &wstatus, WNOHANG)) > 0) {
        container_state_t new_state   = CONTAINER_EXITED;
        int               exit_code   = 0;
        int               exit_signal = 0;
        pthread_t         pt          = 0;
        int               run_fd      = -1;
        control_response_t run_resp;

        memset(&run_resp, 0, sizeof(run_resp));

        if (WIFEXITED(wstatus)) {
            exit_code = WEXITSTATUS(wstatus);
            new_state = CONTAINER_EXITED;
        } else if (WIFSIGNALED(wstatus)) {
            exit_signal = WTERMSIG(wstatus);
            exit_code   = 128 + exit_signal;
            /* Task 4 attribution rule */
            /* stop_requested is checked below after lock */
        }

        pthread_mutex_lock(&ctx->metadata_lock);
        for (container_record_t *c = ctx->containers; c; c = c->next) {
            if (c->host_pid != pid)
                continue;

            if (WIFSIGNALED(wstatus)) {
                if (c->stop_requested) {
                    new_state = CONTAINER_STOPPED;
                } else if (exit_signal == SIGKILL) {
                    new_state = CONTAINER_KILLED; /* hard-limit kill */
                } else {
                    new_state = CONTAINER_EXITED;
                }
            }

            c->state       = new_state;
            c->exit_code   = exit_code;
            c->exit_signal = exit_signal;

            /* Unregister from kernel monitor */
            if (ctx->monitor_fd >= 0)
                unregister_from_monitor(ctx->monitor_fd, c->id, pid);

            /* Collect thread handle and run client fd before releasing lock */
            pt     = c->producer_thread;
            run_fd = c->run_client_fd;
            c->producer_thread = 0;
            c->run_client_fd   = -1;

            if (run_fd >= 0) {
                run_resp.status = exit_code;
                snprintf(run_resp.message, sizeof(run_resp.message),
                         "container '%s' exited: state=%s exit_code=%d signal=%d",
                         c->id, state_to_string(new_state),
                         exit_code, exit_signal);
            }
            break;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        /* Join producer thread outside lock (it exits quickly on EOF) */
        if (pt)
            pthread_join(pt, NULL);

        /* Notify run client outside lock */
        if (run_fd >= 0) {
            (void)write(run_fd, &run_resp, sizeof(run_resp));
            close(run_fd);
        }
    }
}

/* Dispatch one accepted client connection. */
static void handle_client(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t  req;
    control_response_t resp;
    ssize_t            n;

    n = read(client_fd, &req, sizeof(req));
    if (n != (ssize_t)sizeof(req)) {
        close(client_fd);
        return;
    }
    memset(&resp, 0, sizeof(resp));

    switch (req.kind) {

    case CMD_START: {
        int running = 0;
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *ex = find_container(ctx, req.container_id);
        if (ex && (ex->state == CONTAINER_RUNNING ||
                   ex->state == CONTAINER_STARTING))
            running = 1;
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (running) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' already running", req.container_id);
        } else if (launch_container(ctx, &req) == 0) {
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "started container '%s' (pid %d)",
                     req.container_id,
                     find_container(ctx, req.container_id)
                         ? find_container(ctx, req.container_id)->host_pid : -1);
        } else {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "failed to start container '%s'", req.container_id);
        }
        (void)write(client_fd, &resp, sizeof(resp));
        close(client_fd);
        break;
    }

    case CMD_RUN: {
        int running = 0;
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *ex = find_container(ctx, req.container_id);
        if (ex && (ex->state == CONTAINER_RUNNING ||
                   ex->state == CONTAINER_STARTING))
            running = 1;
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (running) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' already running", req.container_id);
            (void)write(client_fd, &resp, sizeof(resp));
            close(client_fd);
        } else if (launch_container(ctx, &req) != 0) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "failed to start container '%s'", req.container_id);
            (void)write(client_fd, &resp, sizeof(resp));
            close(client_fd);
        } else {
            /* Store client_fd; SIGCHLD handler sends the response when done */
            pthread_mutex_lock(&ctx->metadata_lock);
            container_record_t *rec = find_container(ctx, req.container_id);
            if (rec)
                rec->run_client_fd = client_fd;
            else
                close(client_fd);
            pthread_mutex_unlock(&ctx->metadata_lock);
        }
        break;
    }

    case CMD_PS: {
        char buf[4096];
        int  off = 0;

        pthread_mutex_lock(&ctx->metadata_lock);
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%-16s %-7s %-10s %-6s %-10s %-10s %s\n",
                        "ID", "PID", "STATE", "EXIT",
                        "SOFT_MIB", "HARD_MIB", "STARTED");
        for (container_record_t *c = ctx->containers; c; c = c->next) {
            char       timebuf[32];
            struct tm *tm_info = localtime(&c->started_at);
            strftime(timebuf, sizeof(timebuf), "%H:%M:%S", tm_info);
            off += snprintf(buf + off, sizeof(buf) - off,
                            "%-16s %-7d %-10s %-6d %-10lu %-10lu %s\n",
                            c->id, c->host_pid,
                            state_to_string(c->state),
                            c->exit_code,
                            c->soft_limit_bytes >> 20,
                            c->hard_limit_bytes >> 20,
                            timebuf);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        resp.status = 0;
        strncpy(resp.message, buf, sizeof(resp.message) - 1);
        (void)write(client_fd, &resp, sizeof(resp));
        close(client_fd);
        break;
    }

    case CMD_LOGS: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *rec = find_container(ctx, req.container_id);
        if (rec) {
            resp.status = 0;
            strncpy(resp.message, rec->log_path, sizeof(resp.message) - 1);
        } else {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' not found", req.container_id);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        (void)write(client_fd, &resp, sizeof(resp));
        close(client_fd);
        break;
    }

    case CMD_STOP: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *rec = find_container(ctx, req.container_id);
        if (!rec) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' not found", req.container_id);
        } else if (rec->state != CONTAINER_RUNNING &&
                   rec->state != CONTAINER_STARTING) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "container '%s' is not running (state=%s)",
                     req.container_id, state_to_string(rec->state));
        } else {
            /* Set flag BEFORE sending signal so SIGCHLD handler can
             * correctly attribute the exit as a manual stop. */
            rec->stop_requested = 1;
            kill(rec->host_pid, SIGTERM);
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "sent SIGTERM to container '%s' (pid %d)",
                     req.container_id, rec->host_pid);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        (void)write(client_fd, &resp, sizeof(resp));
        close(client_fd);
        break;
    }

    default:
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message),
                 "unknown command %d", req.kind);
        (void)write(client_fd, &resp, sizeof(resp));
        close(client_fd);
        break;
    }
}

/* Orderly supervisor shutdown: SIGTERM all running containers, wait up
 * to 5 s, then SIGKILL stragglers. */
static void supervisor_shutdown(supervisor_ctx_t *ctx)
{
    int tries;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (container_record_t *c = ctx->containers; c; c = c->next) {
        if (c->state == CONTAINER_RUNNING || c->state == CONTAINER_STARTING) {
            c->stop_requested = 1;
            kill(c->host_pid, SIGTERM);
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    for (tries = 50; tries > 0; tries--) {
        int any = 0;
        reap_children(ctx);
        pthread_mutex_lock(&ctx->metadata_lock);
        for (container_record_t *c = ctx->containers; c; c = c->next)
            if (c->state == CONTAINER_RUNNING || c->state == CONTAINER_STARTING)
                any = 1;
        pthread_mutex_unlock(&ctx->metadata_lock);
        if (!any)
            break;
        usleep(100000);
    }

    /* Force kill anything left */
    pthread_mutex_lock(&ctx->metadata_lock);
    for (container_record_t *c = ctx->containers; c; c = c->next)
        if (c->state == CONTAINER_RUNNING || c->state == CONTAINER_STARTING)
            kill(c->host_pid, SIGKILL);
    pthread_mutex_unlock(&ctx->metadata_lock);

    reap_children(ctx);
}

/* ================================================================
 * Supervisor entry point
 * ================================================================ */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t   ctx;
    struct sockaddr_un addr;
    struct sigaction   sa;
    int                rc;

    (void)rootfs; /* template rootfs noted but containers use their own copy */

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* Create log directory */
    mkdir(LOG_DIR, 0755);

    /* Open kernel monitor device (gracefully absent if module not loaded) */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr,
                "Note: /dev/container_monitor unavailable "
                "(kernel module not loaded)\n");

    /* Self-pipe for signal delivery into select() loop */
    if (pipe(g_signal_pipe) != 0) {
        perror("pipe");
        goto cleanup;
    }
    fcntl(g_signal_pipe[0], F_SETFL, O_NONBLOCK);
    fcntl(g_signal_pipe[1], F_SETFL, O_NONBLOCK);

    /* Install signal handlers */
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = supervisor_signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sigaction(SIGCHLD, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* UNIX domain socket for CLI control channel */
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto cleanup;
    }
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("bind");
        goto cleanup;
    }
    if (listen(ctx.server_fd, 16) != 0) {
        perror("listen");
        goto cleanup;
    }
    fcntl(ctx.server_fd, F_SETFL, O_NONBLOCK);

    /* Start logging consumer thread */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread,
                        &ctx.log_buffer);
    if (rc != 0) {
        fprintf(stderr, "pthread_create (logger): %s\n", strerror(rc));
        goto cleanup;
    }

    fprintf(stdout, "Supervisor running. Control socket: %s\n", CONTROL_PATH);
    fflush(stdout);

    /* ---- Main event loop ---- */
    while (!ctx.should_stop) {
        fd_set         rfds;
        int            maxfd;
        struct timeval tv;

        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd,     &rfds);
        FD_SET(g_signal_pipe[0],  &rfds);
        maxfd = (ctx.server_fd > g_signal_pipe[0])
                    ? ctx.server_fd
                    : g_signal_pipe[0];

        tv.tv_sec  = 1;
        tv.tv_usec = 0;

        if (select(maxfd + 1, &rfds, NULL, NULL, &tv) < 0 && errno != EINTR)
            break;

        /* Drain signal pipe */
        if (FD_ISSET(g_signal_pipe[0], &rfds)) {
            unsigned char sig;
            while (read(g_signal_pipe[0], &sig, 1) == 1) {
                if (sig == SIGCHLD) {
                    reap_children(&ctx);
                } else if (sig == SIGINT || sig == SIGTERM) {
                    fprintf(stderr,
                            "\nSupervisor: signal %d received, "
                            "shutting down...\n", (int)sig);
                    ctx.should_stop = 1;
                }
            }
        }

        /* Accept incoming CLI connection */
        if (!ctx.should_stop && FD_ISSET(ctx.server_fd, &rfds)) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd >= 0)
                handle_client(&ctx, client_fd);
        }
    }

    /* Orderly teardown */
    supervisor_shutdown(&ctx);

    /* Stop logging pipeline; consumer drains buffer before returning */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    /* Free container metadata */
    pthread_mutex_lock(&ctx.metadata_lock);
    {
        container_record_t *c = ctx.containers;
        while (c) {
            container_record_t *nxt = c->next;
            if (c->run_client_fd >= 0)
                close(c->run_client_fd);
            free(c);
            c = nxt;
        }
        ctx.containers = NULL;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    fprintf(stdout, "Supervisor exited cleanly.\n");

cleanup:
    if (ctx.server_fd  >= 0) close(ctx.server_fd);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    if (g_signal_pipe[0] >= 0) { close(g_signal_pipe[0]); g_signal_pipe[0] = -1; }
    if (g_signal_pipe[1] >= 0) { close(g_signal_pipe[1]); g_signal_pipe[1] = -1; }
    unlink(CONTROL_PATH);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

/* ================================================================
 * Client-side: control request sender
 *
 * Connects to the supervisor's UNIX socket, sends one request,
 * reads one response.  For CMD_RUN the connection stays open until
 * the supervisor sends the final exit status.
 * ================================================================ */
static int connect_to_supervisor(void)
{
    struct sockaddr_un addr;
    int                fd;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("connect (is supervisor running?)");
        close(fd);
        return -1;
    }
    return fd;
}

static int send_control_request(const control_request_t *req)
{
    int                fd;
    control_response_t resp;
    ssize_t            n;

    fd = connect_to_supervisor();
    if (fd < 0)
        return 1;

    n = write(fd, req, sizeof(*req));
    if (n != (ssize_t)sizeof(*req)) {
        perror("write");
        close(fd);
        return 1;
    }

    n = read(fd, &resp, sizeof(resp));
    close(fd);

    if (n != (ssize_t)sizeof(resp)) {
        fprintf(stderr, "Incomplete response from supervisor\n");
        return 1;
    }

    /* CMD_LOGS: supervisor returns log file path; print the file contents */
    if (req->kind == CMD_LOGS && resp.status == 0) {
        FILE *f = fopen(resp.message, "r");
        if (!f) {
            fprintf(stderr, "Cannot open log file: %s\n", resp.message);
            return 1;
        }
        char line[512];
        while (fgets(line, sizeof(line), f))
            fputs(line, stdout);
        fclose(f);
        return 0;
    }

    printf("%s\n", resp.message);
    return (resp.status == 0) ? 0 : 1;
}

/* Signal handler used only by the run-command client */
static void run_client_sighandler(int signo)
{
    (void)signo;
    g_run_stop_requested = 1;
}

/* ================================================================
 * CLI command handlers
 * ================================================================ */

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command>"
                " [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind              = CMD_START;
    req.soft_limit_bytes  = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes  = DEFAULT_HARD_LIMIT;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,        argv[3], sizeof(req.rootfs)        - 1);
    strncpy(req.command,       argv[4], sizeof(req.command)       - 1);

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t  req;
    control_response_t resp;
    struct sigaction   sa;
    int                fd;
    ssize_t            n;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command>"
                " [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind             = CMD_RUN;
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,        argv[3], sizeof(req.rootfs)        - 1);
    strncpy(req.command,       argv[4], sizeof(req.command)       - 1);

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    /* Save container id for stop forwarding */
    strncpy(g_run_id, req.container_id, CONTAINER_ID_LEN - 1);

    /* Install SIGINT/SIGTERM handler for forwarding */
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = run_client_sighandler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    fd = connect_to_supervisor();
    if (fd < 0)
        return 1;

    n = write(fd, &req, sizeof(req));
    if (n != (ssize_t)sizeof(req)) {
        perror("write");
        close(fd);
        return 1;
    }

    /* Block waiting for container exit notification.
     * On EINTR (from SIGINT/SIGTERM): forward stop and keep waiting. */
    while (1) {
        n = read(fd, &resp, sizeof(resp));
        if (n < 0 && errno == EINTR) {
            if (g_run_stop_requested) {
                /* Forward stop via a new connection */
                control_request_t  stop_req;
                control_response_t stop_resp;
                int                sfd;

                memset(&stop_req, 0, sizeof(stop_req));
                stop_req.kind = CMD_STOP;
                strncpy(stop_req.container_id, g_run_id,
                        CONTAINER_ID_LEN - 1);

                sfd = connect_to_supervisor();
                if (sfd >= 0) {
                    (void)write(sfd, &stop_req, sizeof(stop_req));
                    (void)read(sfd,  &stop_resp, sizeof(stop_resp));
                    close(sfd);
                }
                g_run_stop_requested = 0;
            }
            continue;
        }
        break;
    }

    close(fd);

    if (n != (ssize_t)sizeof(resp)) {
        fprintf(stderr, "Incomplete response from supervisor\n");
        return 1;
    }

    printf("%s\n", resp.message);
    return resp.status; /* propagate container exit code */
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

/* ================================================================
 * main
 * ================================================================ */
int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
