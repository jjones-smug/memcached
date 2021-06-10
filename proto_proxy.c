/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Functions for handling the proxy layer. wraps text protocols
 */

#include <string.h>
#include <stdlib.h>

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#if defined(__linux__)
#define USE_EVENTFD 1
#include <sys/eventfd.h>
#endif

#include "memcached.h"
#include "proto_proxy.h"
#include "proto_text.h"
#include "murmur3_hash.h"
#include "queue.h"
#define XXH_INLINE_ALL // modifier for xxh3's include below
#include "xxhash.h"

// TODO: better if an init option turns this on/off.
#ifdef PROXY_DEBUG
#define P_DEBUG(...) \
    do { \
        fprintf(stderr, __VA_ARGS__); \
    } while (0)
#else
#define P_DEBUG(...)
#endif

// FIXME: do include dir properly.
#include "vendor/mcmc/mcmc.h"

#define ENDSTR "END\r\n"
#define ENDLEN sizeof(ENDSTR)-1

#define MCP_THREAD_UPVALUE 1
#define MCP_ATTACH_UPVALUE 2

// NOTE: unused right now.
//#define PROXY_EVENT_IO_THREADS 4

// all possible commands.
#define CMD_FIELDS \
    X(CMD_MG) \
    X(CMD_MS) \
    X(CMD_MD) \
    X(CMD_MN) \
    X(CMD_MA) \
    X(CMD_ME) \
    X(CMD_GET) \
    X(CMD_GAT) \
    X(CMD_SET) \
    X(CMD_ADD) \
    X(CMD_CAS) \
    X(CMD_GETS) \
    X(CMD_GATS) \
    X(CMD_INCR) \
    X(CMD_DECR) \
    X(CMD_TOUCH) \
    X(CMD_APPEND) \
    X(CMD_DELETE) \
    X(CMD_REPLACE) \
    X(CMD_PREPEND) \
    X(CMD_END_STORAGE) \
    X(CMD_QUIT) \
    X(CMD_STATS) \
    X(CMD_SLABS) \
    X(CMD_WATCH) \
    X(CMD_LRU) \
    X(CMD_VERSION) \
    X(CMD_SHUTDOWN) \
    X(CMD_EXTSTORE) \
    X(CMD_FLUSH_ALL) \
    X(CMD_VERBOSITY) \
    X(CMD_LRU_CRAWLER) \
    X(CMD_REFRESH_CERTS) \
    X(CMD_CACHE_MEMLIMIT)

#define X(name) name,
enum proxy_defines {
    P_OK = 0,
    CMD_FIELDS
    CMD_SIZE, // used to define array size for command hooks.
    CMD_ANY, // override _all_ commands
    CMD_ANY_STORAGE, // override commands specific to key storage.
};
#undef X

// certain classes of ascii commands have similar parsing (ie;
// get/gets/gat/gats). Use types so we don't have to test a ton of them.
enum proxy_cmd_types {
    CMD_TYPE_GENERIC = 0,
    CMD_TYPE_GET, // get/gets/gat/gats
    CMD_TYPE_UPDATE, // add/set/cas/prepend/append/replace
    CMD_TYPE_META, // m*'s.
};

struct proxy_hook {
    // TODO: C func ptr. If non-null, call directly instead of via lua.
    int lua_ref;
    bool is_lua; // pull the lua reference and call it as a lua function.
};

typedef uint32_t (*hash_selector_func)(const void *key, size_t len, void *ctx);
struct proxy_hash_caller {
    hash_selector_func selector_func;
    void *ctx;
};

// A default hash function for backends.
static uint32_t mcplib_hashfunc_murmur3_func(const void *key, size_t len, void *ctx) {
    return MurmurHash3_x86_32(key, len);
}
static struct proxy_hash_caller mcplib_hashfunc_murmur3 = { mcplib_hashfunc_murmur3_func, NULL};

typedef struct _io_pending_proxy_t io_pending_proxy_t;
typedef struct proxy_event_thread_s proxy_event_thread_t;

enum mcp_backend_states {
    mcp_backend_read = 0, // waiting to read any response
    mcp_backend_read_end, // looking for an "END" marker for GET
    mcp_backend_want_read, // read more data to complete command
    mcp_backend_next, // advance to the next IO
};

typedef struct mcp_backend_s mcp_backend_t;
typedef struct mcp_request_s mcp_request_t;
typedef struct mcp_parser_s mcp_parser_t;

// function for finalizing the parsing of a request.
struct mcp_parser_set_s {
    uint32_t flags;
    int exptime;
};

struct mcp_parser_get_s {
    int exptime; // in cases of gat/gats.
};

// Note that we must use offsets into request for tokens,
// as *request can change between parsing and later accessors.
// TODO: just use uint16_t off/len token array?
struct mcp_parser_s {
    int command;
    int parsed; // how far into the request we parsed already
    const char *request;
    void *vbuf; // temporary buffer for holding value lengths.
    int cmd_type; // command class.
    int reqlen; // full length of request buffer.
    int vlen;
    int key; // offset of the key.
    int16_t klen; // length of key.
    bool has_space; // a space was found after the command token.
    union {
        struct mcp_parser_set_s set;
        struct mcp_parser_get_s get;
    } t;
};

#define MCP_PARSER_KEY(pr) (&pr.request[pr.key])

// TODO: need to confirm that c->rbuf is safe to use the whole time.
// - I forgot what this was already? need to re-check. have addressed other
// prior comments already.
#define MAX_REQ_TOKENS 2
struct mcp_request_s {
    mcp_parser_t pr; // non-lua-specific parser handling.
    mcp_backend_t *be; // backend handling this request.
    bool lua_key; // if we've pushed the key to lua.
    bool ascii_multiget; // ascii multiget mode. (hide errors/END)
};

typedef STAILQ_HEAD(io_head_s, _io_pending_proxy_t) io_head_t;
#define MAX_IPLEN 45
#define MAX_PORTLEN 6
struct mcp_backend_s {
    char ip[MAX_IPLEN+1];
    char port[MAX_PORTLEN+1];
    double weight;
    int depth;
    pthread_mutex_t mutex; // covers stack.
    proxy_event_thread_t *event_thread; // event thread owning this backend.
    void *client; // mcmc client
    STAILQ_ENTRY(mcp_backend_s) be_next; // stack for backends
    io_head_t io_head; // stack of requests.
    char *rbuf; // TODO: from thread's rbuf cache.
    struct event event; // libevent
    enum mcp_backend_states state; // readback state machine
    bool connecting; // in the process of an asynch connection.
    bool can_write; // recently got a WANT_WRITE or are connecting.
    bool stacked; // if backend already queued for syscalls.
};
typedef STAILQ_HEAD(be_head_s, mcp_backend_s) be_head_t;

typedef struct proxy_event_io_thread_s proxy_event_io_thread_t;
struct proxy_event_thread_s {
    pthread_t thread_id;
    struct event_base *base;
    struct event notify_event; // listen event for the notify pipe.
    pthread_mutex_t mutex; // covers stack.
    pthread_cond_t cond; // condition to wait on while stack drains.
    io_head_t io_head_in; // inbound requests to process.
    be_head_t be_head; // stack of backends for processing.
    mcp_backend_t *iter; // used as an iterator through the be list
    proxy_event_io_thread_t *bt; // array of io threads.
#ifdef USE_EVENTFD
    eventfd_t event_fd;
#else
    int notify_receive_fd;
    int notify_send_fd;
#endif
};

// threads owned by an event thread for submitting syscalls.
struct proxy_event_io_thread_s {
    pthread_t thread_id;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    proxy_event_thread_t *ev;
};

typedef struct {
    mcmc_resp_t resp;
    int status; // status code from mcmc_read()
    item *it; // for buffering large responses.
    char *buf; // response line + potentially value.
    size_t blen; // total size of the value to read.
    int bread; // amount of bytes read into value so far.
} mcp_resp_t;

// re-cast an io_pending_t into this more descriptive structure.
// the first few items _must_ match the original struct.
struct _io_pending_proxy_t {
    io_queue_t *q;
    conn *c;
    mc_resp *resp;  // original struct ends here

    struct _io_pending_proxy_t *next; // stack for IO submission
    STAILQ_ENTRY(_io_pending_proxy_t) io_next; // stack for backends
    int coro_ref; // lua registry reference to the coroutine
    lua_State *coro; // pointer directly to the coroutine
    mcp_backend_t *backend; // backend server to request from
    struct iovec iov[2]; // request string + tail buffer
    int iovcnt; // 1 or 2...
    mcp_resp_t *client_resp; // reference (currently pointing to a lua object)
    bool flushed; // whether we've fully written this request to a backend.
    bool ascii_multiget; // passed on from mcp_r_t
};

// TODO: hash func should take an initializer
// so we can hash a few things together.
// also, 64bit? 32bit? either?
// TODO: hash selectors should hash to a list of "Things"
// things can be anything callable: lua funcs, hash selectors, backends.
// TODO: does *be have to be a sub-struct? how stable are userdata pointers?
// https://stackoverflow.com/questions/38718475/lifetime-of-lua-userdata-pointers
// - says no.
typedef struct {
    int ref; // luaL_ref reference.
    mcp_backend_t *be;
} mcp_hash_selector_be_t;

// TODO: hash/compare func ptr
// void *ctx
// ctx_ref?
typedef struct {
    struct proxy_hash_caller *phc;
    int phc_ref;
    int pool_size;
    mcp_hash_selector_be_t pool[];
} mcp_hash_selector_t;

static int proxy_run_coroutine(lua_State *Lc, mc_resp *resp, io_pending_proxy_t *p, conn *c);
#define PROCESS_MULTIGET true
#define PROCESS_NORMAL false
static void proxy_process_command(conn *c, char *command, size_t cmdlen, bool multiget);
static int _process_request_key(mcp_parser_t *pr);
static int process_request(mcp_parser_t *pr, const char *command, size_t cmdlen);
static void dump_stack(lua_State *L);
static void mcp_queue_io(conn *c, mc_resp *resp, int coro_ref, lua_State *Lc);
static mcp_request_t *mcp_new_request(lua_State *L, mcp_parser_t *pr, const char *command, size_t cmdlen);
static void proxy_backend_handler(const int fd, const short which, void *arg);
static void proxy_event_handler(evutil_socket_t fd, short which, void *arg);
static void *proxy_event_thread(void *arg);
static void proxy_out_errstring(mc_resp *resp, const char *str);
static int _flush_pending_write(mcp_backend_t *be, io_pending_proxy_t *p);
static int _reset_bad_backend(mcp_backend_t *be);
static void _set_event(mcp_backend_t *be, struct event_base *base, int flags, struct timeval t);

/******** EXTERNAL FUNCTIONS ******/
// functions starting with _ are breakouts for the public functions.

struct _dumpbuf {
    size_t size;
    size_t used;
    char *buf;
};

static int _dump_helper(lua_State *L, const void *p, size_t sz, void *ud) {
    (void)L;
    struct _dumpbuf *db = ud;
    if (db->used + sz > db->size) {
        db->size *= 2;
        char *nb = realloc(db->buf, db->size);
        if (nb == NULL) {
            return -1;
        }
        db->buf = nb;
    }
    memcpy(db->buf + db->used, (const char *)p, sz);
    db->used += sz;
    return 0;
}

static const char * _load_helper(lua_State *L, void *data, size_t *size) {
    (void)L;
    struct _dumpbuf *db = data;
    if (db->used == 0) {
        *size = 0;
        return NULL;
    }
    *size = db->used;
    db->used = 0;
    return db->buf;
}

// start the centralized lua state and config thread.
void proxy_init(void) {
    lua_State *L = luaL_newstate();
    settings.proxy_state = L;
    luaL_openlibs(L);
    // NOTE: might need to differentiate the libs yes?
    proxy_register_libs(NULL, L);

    int res = luaL_loadfile(L, settings.proxy_startfile);
    if (res != LUA_OK) {
        fprintf(stderr, "Failed to load proxy_startfile: %s\n", lua_tostring(L, -1));
        exit(EXIT_FAILURE);
    }
    // LUA_OK, LUA_ERRSYNTAX, LUA_ERRMEM, LUA_ERRFILE

    // Now we need to dump the compiled code into bytecode.
    // This will then get loaded into worker threads.
    struct _dumpbuf *db = malloc(sizeof(struct _dumpbuf));
    db->size = 16384;
    db->used = 0;
    db->buf = malloc(db->size);
    lua_dump(L, _dump_helper, db, 0);
    // 0 means no error.
    settings.proxy_code = db;

    // Create/start the backend threads, which we need before servers
    // start getting created.
    // Supporting N event threads should be possible, but it will be a
    // low number of N to avoid too many wakeup syscalls.
    // For now we hardcode to 1.
    proxy_event_thread_t *threads = calloc(1, sizeof(proxy_event_thread_t));
    settings.proxy_threads = threads;
    for (int i = 0; i < 1; i++) {
        proxy_event_thread_t *t = &threads[i];
#ifdef USE_EVENTFD
        t->event_fd = eventfd(0, EFD_NONBLOCK);
        // FIXME: eventfd can fail?
#else
        int fds[2];
        if (pipe(fds)) {
            perror("can't create proxy backend notify pipe");
            exit(1);
        }

        t->notify_receive_fd = fds[0];
        t->notify_send_fd = fds[1];
#endif
        struct event_config *ev_config;
        ev_config = event_config_new();
        event_config_set_flag(ev_config, EVENT_BASE_FLAG_NOLOCK);
        t->base = event_base_new_with_config(ev_config);
        event_config_free(ev_config);
        if (! t->base) {
            fprintf(stderr, "Can't allocate event base\n");
            exit(1);
        }

        // listen for notifications.
        // NULL was thread_libevent_process
        // FIXME: use modern format? (event_assign)
#ifdef USE_EVENTFD
        event_set(&t->notify_event, t->event_fd,
              EV_READ | EV_PERSIST, proxy_event_handler, t);
#else
        event_set(&t->notify_event, t->notify_receive_fd,
              EV_READ | EV_PERSIST, proxy_event_handler, t);
#endif
        event_base_set(t->base, &t->notify_event);
        if (event_add(&t->notify_event, 0) == -1) {
            fprintf(stderr, "Can't monitor libevent notify pipe\n");
            exit(1);
        }

        // incoming request queue.
        STAILQ_INIT(&t->io_head_in);
        pthread_mutex_init(&t->mutex, NULL);
        pthread_cond_init(&t->cond, NULL);

        pthread_create(&t->thread_id, NULL, proxy_event_thread, t);
    }

    // now we complete the data load by calling the function.
    res = lua_pcall(L, 0, LUA_MULTRET, 0);
    if (res != LUA_OK) {
        fprintf(stderr, "Failed to load data into L: %s\n", lua_tostring(L, -1));
        exit(EXIT_FAILURE);
    }

    // call the mcp_config_selectors function to get the central backends.
    lua_getglobal(L, "mcp_config_selectors");

    // TODO: handle explicitly if function is missing.
    lua_pushnil(L); // no "old" config yet.
    if (lua_pcall(L, 1, 1, 0) != LUA_OK) {
        fprintf(stderr, "Failed to execute mcp_config_selectors: %s\n", lua_tostring(L, -1));
        exit(EXIT_FAILURE);
    }

    // result is our main config.
}

// TODO: this will be done differently while implementing config reloading.
static int _copy_hash_selector(lua_State *from, lua_State *to) {
    // from, -3 should have he userdata.
    mcp_hash_selector_t *ss = luaL_checkudata(from, -3, "mcp.hash_selector");
    size_t size = sizeof(mcp_hash_selector_t) + sizeof(mcp_hash_selector_be_t) * ss->pool_size;
    mcp_hash_selector_t *css = lua_newuserdatauv(to, size, 0);
    luaL_setmetatable(to, "mcp.hash_selector");
    // TODO: check css.

    // TODO: we just straight copy the references here, pointing at the
    // mcp.backend's from the config without actually holding proper
    // references!
    // This is done today because there's no code reload.
    // To implement code reload this will need to add references to the
    // original structure to hold it.
    memcpy(css, ss, size);
    return 0;
}

static void _copy_config_table(lua_State *from, lua_State *to);
// (from, -1) is the source value
// should end with (to, -1) being the new value.
// TODO: { foo = "bar", { thing = "foo" } } fails for lua_next() post final
// table.
static void _copy_config_table(lua_State *from, lua_State *to) {
    int type = lua_type(from, -1);
    bool found = false;
    switch (type) {
        case LUA_TNIL:
            lua_pushnil(to);
            break;
        case LUA_TUSERDATA:
            // see dump_stack() - check if it's something we handle.
            if (lua_getmetatable(from, -1) != 0) {
                lua_pushstring(from, "__name");
                if (lua_rawget(from, -2) != LUA_TNIL) {
                    const char *name = lua_tostring(from, -1);
                    if (strcmp(name, "mcp.hash_selector") == 0) {
                        // FIXME: check result
                        _copy_hash_selector(from, to);
                        found = true;
                    }
                }
                lua_pop(from, 2);
            }
            if (!found) {
                fprintf(stderr, "unhandled userdata type\n");
                exit(1);
            }
            break;
        case LUA_TNUMBER:
            // FIXME: since 5.3 there's some sub-thing you need to do to push
            // float vs int.
            lua_pushnumber(to, lua_tonumber(from, -1));
            break;
        case LUA_TSTRING:
            // FIXME: temp var + tolstring worth doing?
            lua_pushlstring(to, lua_tostring(from, -1), lua_rawlen(from, -1));
            break;
        case LUA_TTABLE:
            // TODO: huge table could cause stack exhaustion? have to do
            // checkstack perhaps?
            // TODO: copy the metatable first?
            // TODO: size narr/nrec from old table and use createtable to
            // pre-allocate.
            lua_newtable(to); // throw new table on worker
            int t = lua_absindex(from, -1); // static index of table to copy.
            int nt = lua_absindex(to, -1); // static index of new table.
            lua_pushnil(from); // start iterator for main
            while (lua_next(from, t) != 0) {
                // (key, -2), (val, -1)
                // TODO: check what key is (it can be anything :|)
                // to allow an optimization later lets restrict it to strings
                // and numbers.
                // don't coerce it to a string unless it already is one.
                lua_pushlstring(to, lua_tostring(from, -2), lua_rawlen(from, -2));
                // lua_settable(to, n) - n being the table
                // takes -2 key -1 value, pops both.
                // use lua_absindex(L, -1) and so to convert easier?
                _copy_config_table(from, to); // push next value.
                lua_settable(to, nt);
                lua_pop(from, 1); // drop value, keep key.
            }
            // top of from is now the original table.
            // top of to should be the new table.
            break;
        default:
            // FIXME: error.
            fprintf(stderr, "unhandled type\n");
            exit(1);
    }
}

// Initialize the VM for an individual worker thread.
void proxy_thread_init(LIBEVENT_THREAD *thr) {
    // Create the hook table.
    thr->proxy_hooks = calloc(CMD_SIZE, sizeof(struct proxy_hook));
    if (thr->proxy_hooks == NULL) {
        fprintf(stderr, "Failed to allocate proxy hooks\n");
        exit(EXIT_FAILURE);
    }

    // Initialize the lua state.
    lua_State *L = luaL_newstate();
    thr->L = L;
    luaL_openlibs(L);
    proxy_register_libs(thr, L);

    // load the precompiled config function.
    struct _dumpbuf *db = settings.proxy_code;
    struct _dumpbuf db2; // copy because the helper modifies it.
    memcpy(&db2, db, sizeof(struct _dumpbuf));

    lua_load(L, _load_helper, &db2, "config", NULL);
    // LUA_OK + all errs from loadfile except LUA_ERRFILE.
    //dump_stack(L);
    // - pcall the func (which should load it)
    int res = lua_pcall(L, 0, LUA_MULTRET, 0);
    if (res != LUA_OK) {
        fprintf(stderr, "Failed to load data into L2\n");
        exit(EXIT_FAILURE);
    }

    lua_getglobal(L, "mcp_config_routes");
    // create deepcopy of argument to pass into mcp_config_routes.
    _copy_config_table((lua_State *)settings.proxy_state, L);

    // copied value is in front of route function, now call it.
    if (lua_pcall(L, 1, 1, 0) != LUA_OK) {
        fprintf(stderr, "Failed to execute mcp_config_routes: %s\n", lua_tostring(L, -1));
        exit(EXIT_FAILURE);
    }
}

// ctx_stack is a stack of io_pending_proxy_t's.
void proxy_submit_cb(void *ctx, void *ctx_stack) {
    proxy_event_thread_t *e = ctx;
    io_pending_proxy_t *p = ctx_stack;
    io_head_t head;
    STAILQ_INIT(&head);

    // NOTE: responses get returned in the correct order no matter what, since
    // mc_resp's are linked.
    // we just need to ensure stuff is parsed off the backend in the correct
    // order.
    // So we can do with a single list here, but we need to repair the list as
    // responses are parsed. (in the req_remaining-- section)
    // TODO:
    // - except we can't do that because the deferred IO stack isn't
    // compatible with queue.h.
    // So for now we build the secondary list with an STAILQ, which
    // can be transplanted/etc.
    while (p) {
        // insert into tail so head is oldest request.
        STAILQ_INSERT_TAIL(&head, p, io_next);
        p = p->next;
    }

    // Transfer request stack to event thread.
    pthread_mutex_lock(&e->mutex);
    STAILQ_CONCAT(&e->io_head_in, &head);
    // No point in holding the lock since we're not doing a cond signal.
    pthread_mutex_unlock(&e->mutex);

    // Signal to check queue.
    // TODO: error handling.
#ifdef USE_EVENTFD
    uint64_t u = 1;
    // FIXME: check result? is it ever possible to get a short write/failure
    // for an eventfd?
    if (write(e->event_fd, &u, sizeof(uint64_t)) != sizeof(uint64_t)) {
        assert(1 == 0);
    }
#else
    if (write(e->notify_send_fd, "w", 1) <= 0) {
        assert(1 == 0);
    }
#endif

    return;
}

void proxy_complete_cb(void *ctx, void *ctx_stack) {
    io_pending_proxy_t *p = ctx_stack;

    while (p) {
        io_pending_proxy_t *next = p->next;
        mc_resp *resp = p->resp;
        lua_State *Lc = p->coro;

        // in order to resume we need to remove the objects that were
        // originally returned
        // what's currently on the top of the stack is what we want to keep.
        lua_rotate(Lc, 1, 1);
        // We kept the original results from the yield so lua would not
        // collect them in the meantime. We can drop those now.
        lua_settop(Lc, 1);

        proxy_run_coroutine(Lc, resp, p, NULL);

        // don't need to flatten main thread here, since the coro is gone.

        p = next;
    }
    return;
}

// called from the worker thread as an mc_resp is being freed.
// must let go of the coroutine reference if there is one.
// caller frees the pending IO.
void proxy_finalize_cb(io_pending_t *pending) {
    io_pending_proxy_t *p = (io_pending_proxy_t *)pending;

    // release our coroutine reference.
    // TODO: coroutines are reusable in latest lua. we can stack this onto a freelist
    // after a lua_resetthread(Lc) call.
    if (p->coro_ref) {
        // Note: lua registry is the same for main thread or a coroutine.
        luaL_unref(p->coro, LUA_REGISTRYINDEX, p->coro_ref);
    }
    return;
}

int try_read_command_proxy(conn *c) {
    char *el, *cont;

    if (c->rbytes == 0)
        return 0;

    el = memchr(c->rcurr, '\n', c->rbytes);
    if (!el) {
        if (c->rbytes > 1024) {
            /*
             * We didn't have a '\n' in the first k. This _has_ to be a
             * large multiget, if not we should just nuke the connection.
             */
            char *ptr = c->rcurr;
            while (*ptr == ' ') { /* ignore leading whitespaces */
                ++ptr;
            }

            if (ptr - c->rcurr > 100 ||
                (strncmp(ptr, "get ", 4) && strncmp(ptr, "gets ", 5))) {

                conn_set_state(c, conn_closing);
                return 1;
            }

            // ASCII multigets are unbound, so our fixed size rbuf may not
            // work for this particular workload... For backcompat we'll use a
            // malloc/realloc/free routine just for this.
            if (!c->rbuf_malloced) {
                if (!rbuf_switch_to_malloc(c)) {
                    conn_set_state(c, conn_closing);
                    return 1;
                }
            }
        }

        return 0;
    }
    cont = el + 1;
    // TODO: we don't want to cut the \r\n here. lets see how lua handles
    // non-terminated strings?
    /*if ((el - c->rcurr) > 1 && *(el - 1) == '\r') {
        el--;
    }
    *el = '\0';*/

    assert(cont <= (c->rcurr + c->rbytes));

    c->last_cmd_time = current_time;
    proxy_process_command(c, c->rcurr, cont - c->rcurr, PROCESS_NORMAL);

    c->rbytes -= (cont - c->rcurr);
    c->rcurr = cont;

    assert(c->rcurr <= (c->rbuf + c->rsize));

    return 1;

}

// we buffered a SET of some kind.
void complete_nread_proxy(conn *c) {
    assert(c != NULL);

    conn_set_state(c, conn_new_cmd);

    LIBEVENT_THREAD *thr = c->thread;
    lua_State *L = thr->L;
    lua_State *Lc = lua_tothread(L, -1);
    // FIXME: could use a quicker method to retrieve the request.
    mcp_request_t *rq = luaL_checkudata(Lc, -1, "mcp.request");

    // validate the data chunk.
    if (strncmp((char *)c->item + rq->pr.vlen - 2, "\r\n", 2) != 0) {
        // TODO: error handling.
        lua_settop(L, 0); // clear anything remaining on the main thread.
        return;
    }
    rq->pr.vbuf = c->item;
    c->item = NULL;

    proxy_run_coroutine(Lc, c->resp, NULL, c);

    lua_settop(L, 0); // clear anything remaining on the main thread.

    return;
}

/******** NETWORKING AND INTERNAL FUNCTIONS ******/

// event handler for executing backend requests
static void proxy_event_handler(evutil_socket_t fd, short which, void *arg) {
    proxy_event_thread_t *t = arg;
    io_head_t head;

#ifdef USE_EVENTFD
    uint64_t u;
    if (read(fd, &u, sizeof(uint64_t)) != sizeof(uint64_t)) {
        // FIXME: figure out if this is impossible, and how to handle if not.
        assert(1 == 0);
    }
#else
    char buf[1];
    // TODO: This is a lot more fatal than it should be. can it fail? can
    // it blow up the server?
    // FIXME: a cross-platform method of speeding this up would be nice. With
    // event fds we can queue N events and wakeup once here.
    // If we're pulling one byte out of the pipe at a time here it'll just
    // wake us up too often.
    // If the pipe is O_NONBLOCK then maybe just a larger read would work?
    if (read(fd, buf, 1) != 1) {
        P_DEBUG("%s: pipe read failed\n", __func__);
        return;
    }
#endif

    STAILQ_INIT(&head);
    STAILQ_INIT(&t->be_head);

    // Pull the entire stack of inbound into local queue.
    pthread_mutex_lock(&t->mutex);
    STAILQ_CONCAT(&head, &t->io_head_in);
    pthread_mutex_unlock(&t->mutex);

    int io_count = 0;
    int be_count = 0;
    while (!STAILQ_EMPTY(&head)) {
        io_pending_proxy_t *io = STAILQ_FIRST(&head);
        io->flushed = false;
        mcp_backend_t *be = io->backend;
        // So the backend can retrieve its event base.
        be->event_thread = t;

        // _no_ mutex on backends. they are owned by the event thread.
        STAILQ_REMOVE_HEAD(&head, io_next);
        STAILQ_INSERT_TAIL(&be->io_head, io, io_next);
        be->depth++;
        io_count++;
        if (!be->stacked) {
            be->stacked = true;
            STAILQ_INSERT_TAIL(&t->be_head, be, be_next);
            be_count++;
        }
    }

    if (io_count == 0) {
        //P_DEBUG("%s: no IO's to complete\n", __func__);
        return;
    }
    //P_DEBUG("%s: io/be counts for syscalls [%d/%d]\n", __func__, io_count, be_count);

    /*
    // TODO: see notes on proxy_event_io_thread
    pthread_mutex_lock(&t->mutex);
    // initialize iterator.
    t->iter = STAILQ_FIRST(&t->be_head);
    // FIXME: this loop is only correct if all io threads are definitely
    // waiting before we lock the ev mutex.
    // need to confirm if we have to lock the bt->mutex's first for sure.

    // IO requests are now stacked into per-backend queues.
    // we do this here to avoid needing mutexes on backends.
    for (int x = 0; x < PROXY_EVENT_IO_THREADS; x++) {
        proxy_event_io_thread_t *bt = &t->bt[x];
        pthread_mutex_lock(&bt->mutex);
        pthread_cond_signal(&bt->cond);
        pthread_mutex_unlock(&bt->mutex);
        if (x == be_count-1)
            break;
    }

    // wait for bg threads while iterator is still valid.
    pthread_cond_wait(&t->cond, &t->mutex);
    pthread_mutex_unlock(&t->mutex);
    */

    // Re-walk each backend and check set event as required.
    mcp_backend_t *be = NULL;
    struct timeval tmp_time = {5,0}; // FIXME: temporary hard coded timeout.

    // FIXME: _set_event() is buggy, see notes on function.
    STAILQ_FOREACH(be, &t->be_head, be_next) {
        be->stacked = false;
        int flags = 0;

        if (be->connecting) {
            P_DEBUG("%s: deferring IO pending connecting\n", __func__);
        } else {
            io_pending_proxy_t *io = NULL;
            STAILQ_FOREACH(io, &be->io_head, io_next) {
                flags = _flush_pending_write(be, io);
                if (flags == -1 || flags & EV_WRITE) {
                    break;
                }
            }
        }

        if (flags == -1) {
            _reset_bad_backend(be);
        } else {
            flags = be->can_write ? EV_READ|EV_TIMEOUT : EV_READ|EV_WRITE|EV_TIMEOUT;
            _set_event(be, t->base, flags, tmp_time);
        }
    }

}

// TODO: this is unused while other parts of the code are fleshed out.
// debugged a few race conditions, and as-is it ended up being slower in quick
// tests than running the syscalls inline with the event thread.
// If code is truly stable I will revisit it later.
/*static void *proxy_event_io_thread(void *arg) {
    proxy_event_io_thread_t *t = arg;
    while (1) {
        bool signal = false;
        proxy_event_thread_t *ev = t->ev;
        pthread_mutex_lock(&ev->mutex);
        if (ev->iter == NULL) {
            pthread_mutex_lock(&t->mutex);
            pthread_mutex_unlock(&ev->mutex);

            pthread_cond_wait(&t->cond, &t->mutex);
            pthread_mutex_unlock(&t->mutex);
            continue;
        }

        // Get a backend to process.
        mcp_backend_t *be = ev->iter;
        // bump the iterator for the next thread.
        ev->iter = STAILQ_NEXT(be, be_next);
        if (ev->iter == NULL)
            signal = true;
        pthread_mutex_unlock(&ev->mutex);

        // If we're in a connecting state, simply skip the flush here and let
        // the event thread wait for a write event.
        if (be->connecting) {
            P_DEBUG("%s: deferring IO pending connecting\n", __func__);
        } else {
            // FIXME: move to a function so we can call this from
            // proxy_backend_handler.
            io_pending_proxy_t *io = NULL;
            int flags = 0;
            STAILQ_FOREACH(io, &be->io_head, io_next) {
                flags |= _flush_pending_write(be, io);
                if (flags & EV_WRITE) {
                    break;
                }
            }
        }
        if (signal)
            pthread_cond_signal(&ev->cond);
    }

    return NULL;
}*/

static void *proxy_event_thread(void *arg) {
    proxy_event_thread_t *t = arg;

    // create our dedicated backend threads for syscall fanout.
    /*t->bt = calloc(PROXY_EVENT_IO_THREADS, sizeof(proxy_event_io_thread_t));
    assert(t->bt != NULL); // TODO: unlikely malloc error.
    for (int x = 0;x < PROXY_EVENT_IO_THREADS; x++) {
        proxy_event_io_thread_t *bt = &t->bt[x];
        bt->ev = t;
        pthread_mutex_init(&bt->mutex, NULL);
        pthread_cond_init(&bt->cond, NULL);

        pthread_create(&bt->thread_id, NULL, proxy_event_io_thread, bt);
    }*/

    event_base_loop(t->base, 0);
    event_base_free(t->base);

    // TODO: join bt threads, free array.

    return NULL;
}


// Need a custom function so we can prefix lua strings easily.
// TODO: can this be made not-necessary somehow?
static void proxy_out_errstring(mc_resp *resp, const char *str) {
    size_t len;
    const static char error_prefix[] = "SERVER_ERROR ";
    const static int error_prefix_len = sizeof(error_prefix) - 1;

    assert(resp != NULL);

    resp_reset(resp);
    // avoid noreply since we're throwing important errors.

    // Fill response object with static string.
    len = strlen(str);
    if ((len + error_prefix_len + 2) > WRITE_BUFFER_SIZE) {
        /* ought to be always enough. just fail for simplicity */
        str = "SERVER_ERROR output line too long";
        len = strlen(str);
    }

    char *w = resp->wbuf;
    memcpy(w, error_prefix, error_prefix_len);
    w += error_prefix_len;

    memcpy(w, str, len);
    w += len;

    memcpy(w, "\r\n", 2);
    resp_add_iov(resp, resp->wbuf, len + error_prefix_len + 2);
    return;
}

// Simple error wrapper for common failures.
// lua_error() is a jump so this function never returns
// for clarity add a 'return' after calls to this.
static void proxy_lua_error(lua_State *L, const char *s) {
    lua_pushstring(L, s);
    lua_error(L);
}

static void proxy_lua_ferror(lua_State *L, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    lua_pushfstring(L, fmt, ap);
    va_end(ap);
    lua_error(L);
}

// FIXME: if we use the newer API the various pending checks can be adjusted.
static void _set_event(mcp_backend_t *be, struct event_base *base, int flags, struct timeval t) {
    // FIXME: chicken and egg.
    // can't check if pending if the structure is was calloc'ed (sigh)
    // don't want to double test here. should be able to event_assign but
    // not add anything during initialization, but need the owner thread's
    // event base.
    int pending = 0;
    if (event_initialized(&be->event)) {
        pending = event_pending(&be->event, EV_READ|EV_WRITE|EV_TIMEOUT, NULL);
    }
    if ((pending & (EV_READ|EV_WRITE|EV_TIMEOUT)) != 0) {
            event_del(&be->event); // replace existing event.
    }

    // if we can't write, we could be connecting.
    // TODO: always check for READ in case some commands were sent
    // successfully? The flags could be tracked on *be and reset in the
    // handler, perhaps?
    event_assign(&be->event, base, mcmc_fd(be->client),
            flags, proxy_backend_handler, be);
    event_add(&be->event, &t);
}

// this resumes every yielded coroutine (and re-resumes if necessary).
// called from the worker thread after responses have been pulled from the
// network.
// Flow:
// - the response object should already be on the coroutine stack.
// - fix up the stack.
// - run coroutine.
// - if LUA_YIELD, we need to swap out the pending IO from its mc_resp then call for a queue
// again.
// - if LUA_OK finalize the response and return
// - else set error into mc_resp.
static int proxy_run_coroutine(lua_State *Lc, mc_resp *resp, io_pending_proxy_t *p, conn *c) {
    int nresults = 0;
    int cores = lua_resume(Lc, NULL, 1, &nresults);
    size_t rlen = 0;

    if (cores == LUA_OK) {
        int type = lua_type(Lc, -1);
        if (type == LUA_TUSERDATA) {
            mcp_resp_t *r = luaL_checkudata(Lc, -1, "mcp.response");
            if (r->buf) {
                // response set from C.
                // FIXME: write_and_free() ? it's a bit wrong for here.
                resp->write_and_free = r->buf;
                resp_add_iov(resp, r->buf, r->blen);
                r->buf = NULL;
            } else if (lua_getiuservalue(Lc, -1, 1) != LUA_TNIL) {
                // uservalue slot 1 is pre-created, so we get TNIL instead of
                // TNONE when nothing was set into it.
                const char *s = lua_tolstring(Lc, -1, &rlen);
                size_t l = rlen > WRITE_BUFFER_SIZE ? WRITE_BUFFER_SIZE : rlen;
                memcpy(resp->wbuf, s, l);
                resp_add_iov(resp, resp->wbuf, l);
                lua_pop(Lc, 1);
            } else if (r->status != MCMC_OK) {
                proxy_out_errstring(resp, "backend failure");
            } else {
                // TODO: double check how this can get here?
                // MCMC_OK but no buffer and no internal value set? still an
                // error?
                P_DEBUG("%s: unhandled response\n", __func__);
            }
        } else if (type == LUA_TSTRING) {
            // response is a raw string from lua.
            const char *s = lua_tolstring(Lc, -1, &rlen);
            size_t l = rlen > WRITE_BUFFER_SIZE ? WRITE_BUFFER_SIZE : rlen;
            memcpy(resp->wbuf, s, l);
            resp_add_iov(resp, resp->wbuf, l);
            lua_pop(Lc, 1);
        } else {
            proxy_out_errstring(resp, "bad response");
        }
    } else if (cores == LUA_YIELD) {
        // need to remove and free the io_pending, since c->resp owns it.
        // so we call mcp_queue_io() again and let it override the
        // mc_resp's io_pending object.

        int coro_ref = 0;
        mc_resp *resp;
        if (p != NULL) {
            coro_ref = p->coro_ref;
            resp = p->resp;
            c = p->c;
            do_cache_free(p->c->thread->io_cache, p);
            // *p is now dead.
        } else {
            // yielding from a top level call to the coroutine,
            // so we need to grab a reference to the coroutine thread.
            // TODO: make this more explicit?
            // we only need to get the reference here, and error conditions
            // should instead drop it, but now it's not obvious to users that
            // we're reaching back into the main thread's stack.
            assert(c != NULL);
            coro_ref = luaL_ref(c->thread->L, LUA_REGISTRYINDEX);
            resp = c->resp;
        }
        // TODO: c only used for cache alloc? push the above into the func?
        mcp_queue_io(c, resp, coro_ref, Lc);
    } else {
        // TODO: log entry for the full failure.
        P_DEBUG("%s: Failed to run coroutine: %s\n", __func__, lua_tostring(Lc, -1));
        proxy_out_errstring(resp, "lua failure");
    }

    return 0;
}

// NOTES:
// - mcp_backend_read: grab req_stack_head, do things
// read -> next, want_read -> next | read_end, etc.
// issue: want read back to read_end as necessary. special state?
//   - it's fine: p->client_resp->type.
// - mcp_backend_next: advance, consume, etc.
// TODO: second argument with enum for a specific error.
// - probably just for logging. for app if any of these errors shouldn't
// result in killing the request stack!
static int proxy_backend_drive_machine(mcp_backend_t *be) {
    bool stop = false;
    io_pending_proxy_t *p = NULL;
    mcmc_resp_t tmp_resp; // helper for testing for GET's END marker.
    int flags = 0;

    while (!stop) {
        mcp_resp_t *r;
        int res = 1;
        int remain = 0;
        char *newbuf = NULL;

    switch(be->state) {
        case mcp_backend_read:
            p = STAILQ_FIRST(&be->io_head);
            if (p == NULL) {
                // got a read event, but nothing was queued.
                // probably means a disconnect event.
                // TODO: could probably confirm this by attempting to read the
                // socket, getsockopt, or something else simply for logging or
                // statistical purposes.
                // In this case we know it's going to be a close so error.
                flags = -1;
                stop = true;
                break;
            }
            assert(p != NULL);
            // FIXME: get the buffer from thread?
            // this leaves a permanent buffer on the backend, which is fine
            // unless you have billions of backends.
            if (be->rbuf == NULL) {
                be->rbuf = malloc(READ_BUFFER_SIZE);
                if (be->rbuf == NULL) {
                    flags = -1; // TODO: specific error.
                    stop = true;
                    break;
                }
            }
            r = p->client_resp;

            r->status = mcmc_read(be->client, be->rbuf, READ_BUFFER_SIZE, &r->resp);
            if (r->status != MCMC_OK) {
                P_DEBUG("%s: mcmc_read failed [%d]\n", __func__, r->status);
                if (r->status == MCMC_WANT_READ) {
                    flags |= EV_READ;
                    stop = true;
                    break;
                } else {
                    // TODO: specific error if NOT_CONNECTED.
                    // if NOT_CONNECTED or other error we still need to kill
                    // the stack; reconnecting will leave in unknown state.
                    flags = -1;
                    stop = true;
                    break;
                }
            }

            // we actually don't care about anything but the value length
            // TODO: if vlen != vlen_read, pull an item and copy the data.
            int extra_space = 0;
            switch (r->resp.type) {
                case MCMC_RESP_GET:
                    // We're in GET mode. we only support one key per
                    // GET in the proxy backends, so we need to later check
                    // for an END.
                    extra_space = ENDLEN;
                    break;
                case MCMC_RESP_END:
                    // this is a MISS from a GET request
                    // or final handler from a STAT request.
                    assert(r->resp.vlen == 0);
                    break;
                case MCMC_RESP_META:
                    // we can handle meta responses easily since they're self
                    // contained.
                    break;
                case MCMC_RESP_GENERIC:
                    break;
                // TODO: No-op response?
                default:
                    P_DEBUG("%s: Unhandled response from backend: %d\n", __func__, r->resp.type);
                    // unhandled :(
                    flags = -1;
                    stop = true;
                    break;
            }

            if (res) {
                if (p->ascii_multiget && r->resp.type == MCMC_RESP_END) {
                    // Ascii multiget hack mode; consume END's
                    be->state = mcp_backend_next;
                    break;
                }

                // r->resp.reslen + r->resp.vlen is the total length of the response.
                // TODO: need to associate a buffer with this response...
                // for now lets abuse write_and_free on mc_resp and simply malloc the
                // space we need, stuffing it into the resp object.

                r->blen = r->resp.reslen + r->resp.vlen;
                r->buf = malloc(r->blen + extra_space);
                if (r->buf == NULL) {
                    flags = -1; // TODO: specific error.
                    stop = true;
                    break;
                }

                if (r->resp.vlen == 0 || r->resp.vlen == r->resp.vlen_read) {
                    // TODO: some mcmc func for pulling the whole buffer?
                    memcpy(r->buf, be->rbuf, r->blen);
                } else {
                    // TODO: mcmc func for pulling the res off the buffer?
                    memcpy(r->buf, be->rbuf, r->resp.reslen);
                    // got a partial read on the value, pull in the rest.
                    r->bread = 0;
                    r->status = mcmc_read_value(be->client, r->buf+r->resp.reslen, r->resp.vlen, &r->bread);
                    if (r->status == MCMC_OK) {
                        // all done copying data.
                    } else if (r->status == MCMC_WANT_READ) {
                        // need to retry later.
                        be->state = mcp_backend_want_read;
                        flags |= EV_READ;
                        stop = true;
                        break;
                        // TODO: stream larger values' chunks?
                    } else {
                        P_DEBUG("%s: mcmc_read_value error: %d\n", __func__, r->status);
                        flags = -1; // TODO: specific error.
                        stop = true;
                        break;
                    }
                }
            } else {
                // TODO: no response read?
                // nothing currently sets res to 0. should remove if that
                // never comes up and handle the error entirely above.
                P_DEBUG("%s: no response read from backend", __func__);
                flags = -1;
                stop = true;
                break;
            }

            if (r->resp.type == MCMC_RESP_GET) {
                be->state = mcp_backend_read_end;
            } else {
                be->state = mcp_backend_next;
            }

            break;
        case mcp_backend_read_end:
            p = STAILQ_FIRST(&be->io_head);
            r = p->client_resp;
            // we need to advance the buffer and ensure the next data
            // in the stream is "END\r\n"
            // if not, the stack is desynced and we lose it.
            newbuf = mcmc_buffer_consume(be->client, &remain);

            if (remain > ENDLEN) {
                // enough bytes in the buffer for our potential END
                // marker, so lets avoid an unnecessary memmove.
            } else if (remain != 0) {
                // TODO: don't necessarily need to shovel the buffer.
                memmove(be->rbuf, newbuf, remain);
                newbuf = be->rbuf;
            } else {
                newbuf = be->rbuf;
            }

            // TODO: WANT_READ can happen here.
            r->status = mcmc_read(be->client, newbuf, READ_BUFFER_SIZE-remain, &tmp_resp);
            if (r->status != MCMC_OK) {
                flags = -1; // TODO: specific error.
                stop = true;
                break;
            } else if (tmp_resp.type != MCMC_RESP_END) {
                // TODO: specific error about protocol desync
                flags = -1;
                stop = true;
                break;
            } else {
                // response is good.
                // FIXME: copy what the server actually sent?
                if (!p->ascii_multiget) {
                    // sigh... if part of a multiget we need to eat the END
                    // markers down here.
                    memcpy(r->buf+r->blen, ENDSTR, ENDLEN);
                    r->blen += 5;
                }
            }

            be->state = mcp_backend_next;

            break;
        case mcp_backend_want_read:
            // Continuing a read from earlier
            p = STAILQ_FIRST(&be->io_head);
            r = p->client_resp;
            r->status = mcmc_read_value(be->client, r->buf+r->resp.reslen, r->resp.vlen, &r->bread);
            if (r->status == MCMC_OK) {
                // all done copying data.
                if (r->resp.type == MCMC_RESP_GET) {
                    be->state = mcp_backend_read_end;
                } else {
                    be->state = mcp_backend_next;
                }
            } else if (r->status == MCMC_WANT_READ) {
                // need to retry later.
                flags |= EV_READ;
                stop = true;
            } else {
                flags = -1; // TODO: specific error
                stop = true;
            }

            break;
        case mcp_backend_next:
            // set the head here. when we break the head will be correct.
            STAILQ_REMOVE_HEAD(&be->io_head, io_next);
            be->depth--;
            if (STAILQ_EMPTY(&be->io_head)) {
                // TODO: suspicious of this code. audit harder?
                stop = true;
            }

            // have to do the q->count-- and == 0 and redispatch_conn()
            // stuff here. The moment we call that write we
            // don't own *p anymore.
            // FIXME: are there any other spots where p->q or p's p->next
            // stack are examined from what would be multiple servers at once?
            p->q->count--;
            if (p->q->count == 0) {
                redispatch_conn(p->c);
            }

            // mcmc_buffer_consume() - if leftover, keep processing
            // IO's.
            // if no more data in buffer, need to re-set stack head and re-set
            // event.
            remain = 0;
            // TODO: do we need to yield every N reads?
            newbuf = mcmc_buffer_consume(be->client, &remain);
            if (remain != 0) {
                // data trailing in the buffer, for a different request.
                memmove(be->rbuf, newbuf, remain);
                //P_DEBUG("read buffer remaining: %d\n", remain);
            } else {
                // TODO: STAILQ_EMPTY() sets stop = true above.
                // remove this and re-attempt a read?
                stop = true;
            }

            be->state = mcp_backend_read;

            break;
        default:
            // TODO: at some point (after v1?) this should attempt to recover,
            // though we should only get here from memory corruption and
            // bailing may be the right thing to do.
            fprintf(stderr, "%s: invalid backend state: %d\n", __func__, be->state);
            assert(false);
    } // switch
    } // while

    return flags;
}

// TODO: add a second argument for assigning a specific error to all pending
// IO's (ie; timeout).
// The backend has gotten into a bad state (timed out, protocol desync, or
// some other supposedly unrecoverable error: purge the queue and
// cycle the socket.
// Note that some types of errors may not require flushing the queue and
// should be fixed as they're figured out.
// _must_ be called from within the event thread.
static int _reset_bad_backend(mcp_backend_t *be) {
    io_pending_proxy_t *io = NULL;
    STAILQ_FOREACH(io, &be->io_head, io_next) {
        // TODO: Unsure if this is the best way of surfacing errors to lua,
        // but will do for V1.
        io->client_resp->status = MCMC_ERR;
        io->q->count--;
        if (io->q->count == 0) {
            redispatch_conn(io->c);
        }
    }

    STAILQ_INIT(&be->io_head);

    mcmc_disconnect(be->client);
    int status = mcmc_connect(be->client, be->ip, be->port, MCMC_OPTION_NONBLOCK);
    if (status == MCMC_CONNECTED) {
        // TODO: unexpected but lets let it be here.
        be->connecting = false;
        be->can_write = true;
    } else if (status == MCMC_CONNECTING) {
        be->connecting = true;
        be->can_write = false;
    } else {
        // TODO: failed to immediately re-establish the connection.
        // need to put the BE into a bad/retry state.
        // FIXME: until we get an event to specifically handle connecting and
        // bad server handling, attempt to force a reconnect here the next
        // time a request comes through.
        // The event thread will attempt to write to the backend, fail, then
        // end up in this routine again.
        be->connecting = false;
        be->can_write = true;
    }

    // TODO: configure the event as necessary internally.

    return 0;
}

static int _flush_pending_write(mcp_backend_t *be, io_pending_proxy_t *p) {
    int flags = 0;

    if (p->flushed) {
        return 0;
    }

    ssize_t sent = 0;
    // FIXME: original send function internally tracked how much was sent, but
    // doing this here would require copying all of the iovecs or modify what
    // we supply.
    // this is probably okay but I want to leave a note here in case I get a
    // better idea.
    int status = mcmc_request_writev(be->client, p->iov, p->iovcnt, &sent, 1);
    if (sent > 0) {
        // we need to save progress in case of WANT_WRITE.
        for (int x = 0; x < p->iovcnt; x++) {
            struct iovec *iov = &p->iov[x];
            if (sent >= iov->iov_len) {
                sent -= iov->iov_len;
                iov->iov_len = 0;
            } else {
                iov->iov_len -= sent;
                sent = 0;
                break;
            }
        }
    }

    // request_writev() returns WANT_WRITE if we haven't fully flushed.
    if (status == MCMC_WANT_WRITE) {
        // avoid syscalls for any other queued requests.
        be->can_write = false;
        flags = EV_WRITE;
        // can't continue for now.
    } else if (status != MCMC_OK) {
        flags = -1;
        // TODO: specific error code
        // s->error = code?
    } else {
        flags = EV_READ;
        p->flushed = true;
    }

    return flags;
}

// The libevent backend callback handler.
// If we end up resetting a backend, it will get put back into a connecting
// state.
static void proxy_backend_handler(const int fd, const short which, void *arg) {
    mcp_backend_t *be = arg;
    int flags = EV_TIMEOUT;
    struct timeval tmp_time = {5,0}; // FIXME: temporary hard coded response timeout.

    if (which & EV_TIMEOUT) {
        P_DEBUG("%s: timeout received, killing backend queue\n", __func__);
        _reset_bad_backend(be);
        return;
    }

    if (which & EV_WRITE) {
        be->can_write = true;
        // TODO: move connect routine to its own function?
        // - hard to do right now because we can't (easily?) edit libevent
        // events.
        if (be->connecting) {
            int err = 0;
            // We were connecting, now ensure we're properly connected.
            if (mcmc_check_nonblock_connect(be->client, &err) != MCMC_OK) {
                // kick the bad backend, clear the queue, retry later.
                // FIXME: if a connect fails, anything currently in the queue
                // should be safe to hold up until their timeout.
                _reset_bad_backend(be);
                return;
            }
            be->connecting = false;
            be->state = mcp_backend_read;
        }
        io_pending_proxy_t *io = NULL;
        int res = 0;
        STAILQ_FOREACH(io, &be->io_head, io_next) {
            res = _flush_pending_write(be, io);
            if (res != -1) {
                flags |= res;
                if (flags & EV_WRITE) {
                    break;
                }
            } else {
                break;
            }
        }
        if (res == -1) {
            _reset_bad_backend(be);
            return;
        }
    }

    if (which & EV_READ) {
        int res = proxy_backend_drive_machine(be);
        if (res != -1) {
            flags |= res;
        } else {
            _reset_bad_backend(be);
            return;
        }

#ifdef PROXY_DEBUG
        if (!STAILQ_EMPTY(&be->io_head)) {
            P_DEBUG("backend has leftover IOs: %d\n", be->depth);
        }
#endif
    }

    // Still pending requests to read or write.
    // TODO: need to handle errors from above so we don't go to sleep here.
    if (!STAILQ_EMPTY(&be->io_head)) {
        flags |= EV_READ; // FIXME: might not be necessary here, but ensures we get a disconnect event.
        _set_event(be, be->event_thread->base, flags, tmp_time);
    }
}

static void proxy_process_command(conn *c, char *command, size_t cmdlen, bool multiget) {
    assert(c != NULL);
    LIBEVENT_THREAD *thr = c->thread;
    struct proxy_hook *hooks = thr->proxy_hooks;
    lua_State *L = thr->L;
    mcp_parser_t pr = {0};

    // TODO: logger integration!

    // Avoid doing resp_start() here, instead do it a bit later or as-needed.
    // This allows us to hop over to the internal text protocol parser, which
    // also calls resp_start().
    // Tighter integration later should obviate the need for this, it is not a
    // permanent solution.
    int ret = process_request(&pr, command, cmdlen);
    if (ret != 0) {
        if (!resp_start(c)) {
            conn_set_state(c, conn_closing);
            return;
        }
        proxy_out_errstring(c->resp, "parsing request");
        if (ret == -2) {
            // Kill connection on more critical parse failure.
            conn_set_state(c, conn_closing);
        }
        return;
    }

    struct proxy_hook *hook = &hooks[pr.command];

    if (!hook->is_lua) {
        // need to pass our command string into the internal handler.
        // to minimize the code change, this means allowing it to tokenize the
        // full command. The proxy's indirect parser should be built out to
        // become common code for both proxy and ascii handlers.
        // For now this means we have to null-terminate the command string,
        // then call into text protocol handler.
        // FIXME: use a ptr or something; don't like this code.
        if (cmdlen > 1 && command[cmdlen-2] == '\r') {
            command[cmdlen-2] = '\0';
        } else {
            command[cmdlen-1] = '\0';
        }
        process_command_ascii(c, command);
        return;
    }

    // If ascii multiget, we turn this into a self-calling loop :(
    // create new request with next key, call this func again, then advance
    // original string.
    // might be better to split this function; the below bits turn into a
    // function call, then we don't re-process the above bits in the same way?
    // The way this is detected/passed on is very fragile.
    if (!multiget && pr.cmd_type == CMD_TYPE_GET && pr.has_space) {
        // TODO: need some way to abort this.
        while (pr.klen != 0) {
            char temp[KEY_MAX_LENGTH + 30];
            size_t off = 0;
            switch (pr.command) {
                case CMD_GET:
                    memcpy(temp, "get ", 4);
                    off += 4;
                    break;
                case CMD_GETS:
                    break;
                case CMD_GAT:
                    break;
                case CMD_GATS:
                    break;
            }
            memcpy(temp+off, MCP_PARSER_KEY(pr), pr.klen);
            memcpy(temp+off+pr.klen, "\r\n", 2);
            proxy_process_command(c, temp, off+pr.klen+2, PROCESS_MULTIGET);

            // now advance to the next key.
            _process_request_key(&pr);

            // TODO: somehow we need to chop the END\r\n from each
            // sub-request.
        }

        if (!resp_start(c)) {
            conn_set_state(c, conn_closing);
            return;
        }

        // The above recursions should have created c->resp's in dispatch
        // order.
        // So now we add another one at the end to create the capping END
        // string.
        memcpy(c->resp->wbuf, ENDSTR, ENDLEN);
        resp_add_iov(c->resp, c->resp->wbuf, ENDLEN);

        return;
    }

    if (!resp_start(c)) {
        conn_set_state(c, conn_closing);
        return;
    }

    // start a coroutine.
    // TODO: This can pull from a cache.
    lua_newthread(L);
    lua_State *Lc = lua_tothread(L, -1);
    // leave the thread first on the stack, so we can reference it if needed.
    // pull the lua hook function onto the stack.
    lua_rawgeti(Lc, LUA_REGISTRYINDEX, hook->lua_ref);

    mcp_request_t *rq = mcp_new_request(Lc, &pr, command, cmdlen);
    if (multiget) {
        rq->ascii_multiget = true;
    }
    // TODO: a better indicator of needing nread? pr->has_value?
    // TODO: lift this to a post-processor?
    if (rq->pr.vlen != 0) {
        // relying on temporary malloc's not succumbing as poorly to
        // fragmentation.
        c->item = malloc(rq->pr.vlen);
        if (c->item == NULL) {
            lua_settop(L, 0);
            proxy_out_errstring(c->resp, "out of memory");
            return;
        }
        c->item_malloced = true;
        c->ritem = c->item;
        c->rlbytes = rq->pr.vlen;

        conn_set_state(c, conn_nread);

        // thread coroutine is still on (L, -1)
        // FIXME: could get speedup from stashing Lc ptr.
        return;
    }

    proxy_run_coroutine(Lc, c->resp, NULL, c);

    lua_settop(L, 0); // clear anything remaining on the main thread.
}

// analogue for storage_get_item(); add a deferred IO object to the current
// connection's response object. stack enough information to write to the
// server on the submit callback, and enough to resume the lua state on the
// completion callback.
static void mcp_queue_io(conn *c, mc_resp *resp, int coro_ref, lua_State *Lc) {
    io_queue_t *q = conn_io_queue_get(c, IO_QUEUE_PROXY);

    // stack: request, hash selector. latter just to hold a reference.

    mcp_request_t *rq = luaL_checkudata(Lc, -1, "mcp.request");
    mcp_backend_t *be = rq->be;
    // FIXME: need to check for "if request modified" and recreate it.
    // Use a local function rather than calling __tostring through lua.

    // Then we push a response object, which we'll re-use later.
    // reserve one uservalue for a lua-supplied response.
    mcp_resp_t *r = lua_newuserdatauv(Lc, sizeof(mcp_resp_t), 1);
    if (r == NULL) {
        proxy_lua_error(Lc, "out of memory allocating response");
        return;
    }
    // FIXME: debugging?
    memset(r, 0, sizeof(mcp_resp_t));
    // TODO: check *r
    r->buf = NULL;
    r->blen = 0;

    luaL_getmetatable(Lc, "mcp.response");
    lua_setmetatable(Lc, -2);

    io_pending_proxy_t *p = do_cache_alloc(c->thread->io_cache);
    // FIXME: can this fail?

    // this is a re-cast structure, so assert that we never outsize it.
    assert(sizeof(io_pending_t) >= sizeof(io_pending_proxy_t));
    memset(p, 0, sizeof(io_pending_proxy_t));
    // set up back references.
    p->q = q;
    p->c = c;
    p->resp = resp;
    p->client_resp = r;
    p->flushed = false;
    p->ascii_multiget = rq->ascii_multiget;
    resp->io_pending = (io_pending_t *)p;

    // top of the main thread should be our coroutine.
    // lets grab a reference to it and pop so it doesn't get gc'ed.
    p->coro_ref = coro_ref;

    // we'll drop the pointer to the coro on here to save some CPU
    // on re-fetching it later. The pointer shouldn't change.
    p->coro = Lc;

    // The direct backend object. Lc is holding the reference in the stack
    p->backend = be;

    // The stringified request. This is also referencing into the coroutine
    // stack, which should be safe from gc.
    mcp_parser_t *pr = &rq->pr;
    p->iov[0].iov_base = (char *)pr->request;
    p->iov[0].iov_len = pr->reqlen;
    p->iovcnt = 1;
    if (pr->vlen != 0) {
        p->iov[1].iov_base = pr->vbuf;
        p->iov[1].iov_len = pr->vlen;
        p->iovcnt = 2;
    }

    // link into the batch chain.
    p->next = q->stack_ctx;
    q->stack_ctx = p;
    q->count++;

    return;
}

/******** LUA INTERFACE FUNCTIONS ******/

__attribute__((unused)) static void dump_stack(lua_State *L) {
    int top = lua_gettop(L);
    int i = 1;
    fprintf(stderr, "--TOP OF STACK [%d]\n", top);
    for (; i < top + 1; i++) {
        int type = lua_type(L, i);
        // lets find the metatable of this userdata to identify it.
        if (lua_getmetatable(L, i) != 0) {
            lua_pushstring(L, "__name");
            if (lua_rawget(L, -2) != LUA_TNIL) {
                fprintf(stderr, "--|%d| [%s] (%s)\n", i, lua_typename(L, type), lua_tostring(L, -1));
                lua_pop(L, 2);
                continue;
            }
            lua_pop(L, 2);
        }
        fprintf(stderr, "--|%d| [%s]\n", i, lua_typename(L, type));
    }
    fprintf(stderr, "-----------------\n");
}

// func prototype example:
// static int fname (lua_State *L)
// normal library open:
// int luaopen_mcp(lua_State *L) { }

// resp:ok()
static int mcplib_response_ok(lua_State *L) {
    mcp_resp_t *r = luaL_checkudata(L, -1, "mcp.response");

    if (r->status == MCMC_OK) {
        lua_pushboolean(L, 1);
    } else {
        lua_pushboolean(L, 0);
    }

    return 1;
}

static int mcplib_response_gc(lua_State *L) {
    mcp_resp_t *r = luaL_checkudata(L, -1, "mcp.response");

    // On error/similar we might be holding the read buffer.
    // If the buf is handed off to mc_resp for return, this pointer is NULL
    if (r->buf != NULL) {
        free(r->buf);
    }

    return 0;
}

static int mcplib_backend(lua_State *L) {
    const char *ip = luaL_checkstring(L, -3); // FIXME: checklstring?
    const char *port = luaL_checkstring(L, -2);
    double weight = luaL_checknumber(L, -1);

    // This might shift to internal objects?
    mcp_backend_t *be = lua_newuserdatauv(L, sizeof(mcp_backend_t), 0);
    if (be == NULL) {
        proxy_lua_error(L, "out of memory allocating server");
        return 0;
    }
    
    strncpy(be->ip, ip, MAX_IPLEN);
    strncpy(be->port, port, MAX_PORTLEN);
    be->weight = weight;
    be->depth = 0;
    be->rbuf = NULL;
    STAILQ_INIT(&be->io_head);
    be->state = mcp_backend_read;
    be->connecting = false;
    be->can_write = false;
    be->stacked = false;

    // initialize libevent.
    memset(&be->event, 0, sizeof(be->event));

    // initialize the client
    be->client = malloc(mcmc_size(MCMC_OPTION_BLANK));
    if (be->client == NULL) {
        proxy_lua_error(L, "out of memory allocating backend");
        return 0;
    }
    // TODO: connect elsewhere? Any reason not to immediately shoot off a
    // connect?
    int status = mcmc_connect(be->client, be->ip, be->port, MCMC_OPTION_NONBLOCK);
    if (status == MCMC_CONNECTED) {
        // FIXME: is this possible? do we ever want to allow blocking
        // connections?
        proxy_lua_ferror(L, "unexpectedly connected to backend early: %s:%s\n", be->ip, be->port);
        return 0;
    } else if (status == MCMC_CONNECTING) {
        be->connecting = true;
        be->can_write = false;
    } else {
        proxy_lua_ferror(L, "failed to connect to backend: %s:%s\n", be->ip, be->port);
        return 0;
    }

    luaL_getmetatable(L, "mcp.backend");
    lua_setmetatable(L, -2); // set metatable to userdata.

    return 1;
}

// hs = mcp.hash_selector(pool, hashfunc, [option])
static int mcplib_hash_selector(lua_State *L) {
    int argc = lua_gettop(L);
    luaL_checktype(L, 1, LUA_TTABLE);
    int n = luaL_len(L, 1); // get length of array table

    mcp_hash_selector_t *hs = lua_newuserdatauv(L, sizeof(mcp_hash_selector_t) + sizeof(mcp_hash_selector_be_t) * n, 0);
    // TODO: check hs.
    // FIXME: zero the memory? then __gc will fix up server references on
    // errors.
    hs->pool_size = n;

    luaL_setmetatable(L, "mcp.hash_selector");

    // TODO: ensure to increment refcounts for servers.
    // remember lua arrays are 1 indexed.
    // TODO: we need a second array with luaL_ref()'s to each of the servers.
    // or an array of structs which hold ptr's.
    for (int x = 1; x <= n; x++) {
        mcp_hash_selector_be_t *s = &hs->pool[x-1];
        lua_geti(L, 1, x); // get next server into the stack.
        // TODO: do we leak memory if we bail here?
        // the stack should clear, then release the userdata + etc?
        // - yes it should leak memory for the registry indexed items.
        s->be = luaL_checkudata(L, -1, "mcp.backend");
        s->ref = luaL_ref(L, LUA_REGISTRYINDEX); // references and pops object.
    }

    if (argc > 1) {
        luaL_checktype(L, 2, LUA_TTABLE);
        if (lua_getfield(L, 2, "new") != LUA_TFUNCTION) {
            proxy_lua_error(L, "hash selector missing 'new' function");
            return 0;
        }

        // - now create the copy pool table
        lua_createtable(L, hs->pool_size, 0); // give the new pool table a sizing hint.
        for (int x = 1; x <= hs->pool_size; x++) {
            mcp_backend_t *be = hs->pool[x-1].be;
            lua_createtable(L, 0, 4);
            // stack = [p, h, f, optN, newpool, backend]
            // the key should be fine for id? maybe don't need to duplicate
            // this?
            lua_pushinteger(L, x);
            lua_setfield(L, -2, "id");
            // TODO: need to get hostname (Separate from logical name?)
            // into the backend objects somehow.
            lua_pushstring(L, "unknown");
            lua_setfield(L, -2, "thing");
            lua_pushstring(L, be->ip);
            lua_setfield(L, -2, "addr");
            lua_pushstring(L, be->port);
            lua_setfield(L, -2, "port");
            // TODO: weight/etc?

            // set the backend table into the new pool table.
            lua_rawseti(L, -2, x);
        }

        // - if argc > 2 we have an option.
        // this needs to go after the pool copy in the stack:
        int callargs = 1;
        if (argc > 2) {
            // we can either use lua_insert() or possibly _rotate to shift
            // things into the right place, but simplest is to just copy the
            // option arg to the end of the stack.
            lua_pushvalue(L, 3);
            callargs++;
            //   - stack should be: pool, hash, func, pool, optN
        }

        // call the hash init function.
        // FIXME: if optarg 1 is + argc-2?
        int res = lua_pcall(L, callargs, 2, 0);

        if (res != LUA_OK) {
            lua_error(L); // error should be on the stack already.
            return 0;
        }

        // TODO: validate response arguments.
        // -1 is lightuserdata ptr to the struct (which must be owned by the
        // userdata), which is later used for internal calls.
        hs->phc = lua_touserdata(L, -1);
        lua_pop(L, 1);
        // -2 was userdata we need to hold a reference to.
        hs->phc_ref = luaL_ref(L, LUA_REGISTRYINDEX);
        // UD now popped from stack.
    } else {
        // Use default hash selector if none given.
        hs->phc = &mcplib_hashfunc_murmur3;
    }

    return 1;
}

// hashfunc(request) -> backend(request)
// needs key from request object.
static int mcplib_hash_selector_call(lua_State *L) {
    // internal args are the hash selector (self)
    mcp_hash_selector_t *ss = luaL_checkudata(L, -2, "mcp.hash_selector");
    // then request object.
    mcp_request_t *rq = luaL_checkudata(L, -1, "mcp.request");

    // we have a fast path to the key/length.
    // FIXME: indicator for if request actually has a key token or not.
    const char *key = MCP_PARSER_KEY(rq->pr);
    size_t len = rq->pr.klen;
    uint32_t lookup = ss->phc->selector_func(key, len, ss->phc->ctx);

    // attach the backend to the request object.
    // save CPU cycles over rolling it through lua.
    if (ss->phc->ctx == NULL) {
        // TODO: if NULL, pass in pool_size as ctx?
        // works because the % bit will return an id we can index here.
        // FIXME: temporary? maybe?
        // if no context, what we got back was a hash which we need to modulus
        // against the pool, since the func has no info about the pool.
        rq->be = ss->pool[lookup % ss->pool_size].be;
    } else {
        // else we have a direct id into our pool.
        // the lua modules should "think" in 1 based indexes, so we need to
        // subtract one here.
        // TODO: bother validating the range?
        rq->be = ss->pool[lookup-1].be;
    }

    // now yield request, hash selector up.
    return lua_yield(L, 2);
}

// mcp.attach(mcp.HOOK_NAME, function)
// fill hook structure: if lua function, use luaL_ref() to store the func
static int mcplib_attach(lua_State *L) {
    // Pull the original worker thread out of the shared mcplib upvalue.
    LIBEVENT_THREAD *t = lua_touserdata(L, lua_upvalueindex(MCP_THREAD_UPVALUE));

    int hook = luaL_checkinteger(L, -2);
    // pushvalue to dupe func and etc.
    // can leave original func on stack afterward because it'll get cleared.
    int loop_end = 0;
    int loop_start = 1;
    if (hook == CMD_ANY) {
        // if CMD_ANY we need individually set loop 1 to CMD_SIZE.
        loop_end = CMD_SIZE;
    } else if (hook == CMD_ANY_STORAGE) {
        // if CMD_ANY_STORAGE we only override get/set/etc.
        loop_end = CMD_END_STORAGE;
    } else {
        loop_start = hook;
        loop_end = hook + 1;
    }

    if (lua_isfunction(L, -1)) {
        struct proxy_hook *hooks = t->proxy_hooks;

        for (int x = loop_start; x < loop_end; x++) {
            struct proxy_hook *h = &hooks[x];
            lua_pushvalue(L, -1); // duplicate the function for the ref.
            if (h->lua_ref) {
                // remove existing reference.
                luaL_unref(L, LUA_REGISTRYINDEX, h->lua_ref);
            }

            // pops the function from the stack and leaves us a ref. for later.
            h->lua_ref = luaL_ref(L, LUA_REGISTRYINDEX);
            h->is_lua = true;
        }
    } else {
        proxy_lua_error(L, "Must pass a function to mcp.attach");
        return 0;
    }

    return 0;
}

static void proxy_register_defines(lua_State *L) {
#define X(x) \
    lua_pushinteger(L, x); \
    lua_setfield(L, -2, #x);

    X(P_OK);
    X(CMD_ANY);
    X(CMD_ANY_STORAGE);
    CMD_FIELDS
#undef X
}

/*** REQUEST PARSER AND OBJECT ***/

static int _process_request_key(mcp_parser_t *pr) {
    pr->has_space = false;
    const char *cur = pr->request + pr->parsed;
    int remain = pr->reqlen - (pr->parsed + 2);
    if (remain <= 0) {
        pr->key = 0;
        pr->klen = 0;
        return 0;
    }
    const char *s = memchr(cur, ' ', remain);
    pr->key = cur - pr->request; // key offset.
    if (s != NULL) {
        // key is up to the next space.
        pr->klen = s - cur;
        if (*s == ' ') {
            pr->has_space = true;
        }
    } else {
        pr->klen = remain;
    }
    pr->parsed += pr->klen+1;

    return 0;
}

// TODO: error codes.
static int _process_request_storage(mcp_parser_t *pr) {
    const char *cur = pr->request + pr->parsed;
    // see mcmc.c's _mcmc_parse_value_line() for the trick
    // set <key> <flags> <exptime> <bytes> [noreply]\r\n
    if (!pr->has_space) {
        return -1;
    }

    // find the key. should this be done here or in main parser?
    // here is probably better in the short term since we may end up
    // re-parsing if ultimately passing to internal dispatch.
    const char *s = memchr(cur, ' ', pr->reqlen - (pr->parsed + 2));
    if (s != NULL) {
        // Found another space, which means we at least have a key.
        pr->key = cur - pr->request;
        pr->klen = s - cur;
        cur = s + 1;
    } else {
        return -1;
    }

    errno = 0;
    char *n = NULL;
    uint32_t flags = strtoul(cur, &n, 10);
    if ((errno == ERANGE) || (cur == n) || (*n != ' ')) {
        return -1;
    }
    cur = n;

    errno = 0;
    int exptime = strtol(cur, &n, 10);
    if ((errno == ERANGE) || (cur == n) || (*n != ' ')) {
        return -1;
    }
    cur = n;

    errno = 0;
    int vlen = strtol(cur, &n, 10);
    if ((errno == ERANGE) || (cur == n)) {
        return -1;
    }
    cur = n;

    if (vlen < 0 || vlen > (INT_MAX - 2)) {
       return -1;
    }
    vlen += 2;

    // TODO: if *n is ' ' look for a CAS value.

    pr->vlen = vlen;
    pr->t.set.flags = flags;
    pr->t.set.exptime = exptime;
    // TODO: if next byte has a space, we check for noreply.
    // TODO: ensure last character is \r
    return 0;
}

// TODO: return code ENUM with error types.
// FIXME: the mcp_parser_t bits have ended up being more fragile than I hoped.
// careful zero'ing is required. revisit?
static int process_request(mcp_parser_t *pr, const char *command, size_t cmdlen) {
    // we want to "parse in place" as much as possible, which allows us to
    // forward an unmodified request without having to rebuild it.

    const char *cm = command;
    size_t cl = 0;
    bool has_space;

    const char *s = memchr(command, ' ', cmdlen-2);
    // TODO: has_space -> has_tokens
    // has_space resered for ascii multiget?
    if (s != NULL) {
        cl = s - command;
        has_space = true;
    } else {
        cl = cmdlen - 2; // FIXME: ensure cmdlen can never be < 2?
        has_space = false;
    }
    pr->has_space = has_space;
    pr->parsed = cl + 1;
    pr->request = command;
    pr->reqlen = cmdlen;

    //pr->vlen = 0; // FIXME: remove this once set indicator is decided
    int cmd = -1;
    int type = CMD_TYPE_GENERIC;
    int ret = 0;

    switch (cl) {
        case 0:
        case 1:
            // falls through with cmd as -1. should error.
            break;
        case 2:
            // TODO: meta support is gated on some protocol fixes.
            if (cm[0] == 'm') {
                switch (cm[1]) {
                    case 'g':
                        cmd = CMD_MG;
                        break;
                    case 's':
                        cmd = CMD_MS;
                        // TODO: special mode to read data.
                        // need to parse enough to know how to read.
                        // ms <key> <flags>*\r\n
                        break;
                    case 'd':
                        cmd = CMD_MD;
                        break;
                    case 'n':
                        cmd = CMD_MN;
                        break;
                    case 'a':
                        cmd = CMD_MA;
                        break;
                    case 'e':
                        cmd = CMD_ME;
                        break;
                }
            }
            break;
        case 3:
            if (cm[0] == 'g') {
                if (cm[1] == 'e' && cm[2] == 't') {
                    cmd = CMD_GET;
                    type = CMD_TYPE_GET;
                    ret = _process_request_key(pr);
                }
                if (cm[1] == 'a' && cm[2] == 't') {
                    type = CMD_TYPE_GET;
                    cmd = CMD_GAT;
                    // TODO: gat <exptime> <key>*\r\n
                }
            } else if (cm[0] == 's' && cm[1] == 'e' && cm[2] == 't') {
                cmd = CMD_SET;
                ret = _process_request_storage(pr);
            } else if (cm[0] == 'a' && cm[1] == 'd' && cm[2] == 'd') {
                cmd = CMD_ADD;
                ret = _process_request_storage(pr);
            } else if (cm[0] == 'c' && cm[1] == 'a' && cm[2] == 's') {
                cmd = CMD_CAS;
                ret = _process_request_storage(pr);
            }
            break;
        case 4:
            if (cm[0] == 'g' && cm[1] == 'e' && cm[2] == 't' && cm[3] == 's') {
                cmd = CMD_GETS;
                type = CMD_TYPE_GET;
                ret = _process_request_key(pr);
            } else if (cm[0] == 'i' && cm[1] == 'n' && cm[2] == 'c' && cm[3] == 'r') {
                cmd = CMD_INCR;
                // TODO: incr <key> <value>
            } else if (cm[0] == 'd' && cm[1] == 'e' && cm[2] == 'c' && cm[3] == 'r') {
                cmd = CMD_DECR;
                // TODO: decr <key> <value>
            } else if (cm[0] == 'g' && cm[1] == 'a' && cm[2] == 't' && cm[3] == 's') {
                cmd = CMD_GATS;
                type = CMD_TYPE_GET;
                // TODO: gats <exptime> <key>*\r\n
            } else if (strncmp(cm, "quit", 4) == 0) {
                cmd = CMD_QUIT;
            }
            break;
        case 5:
            if (strncmp(cm, "touch", 5) == 0) {
                cmd = CMD_TOUCH;
                // TODO: touch <key> <exptime>
                ret = _process_request_key(pr);
            } else if (strncmp(cm, "stats", 5) == 0) {
                cmd = CMD_STATS;
                ret = _process_request_key(pr);
                // :key() should give the stats sub-command?
            }
            break;
        case 6:
            if (strncmp(cm, "delete", 6) == 0) {
                cmd = CMD_DELETE;
                ret = _process_request_key(pr);
            } else if (strncmp(cm, "append", 6) == 0) {
                cmd = CMD_APPEND;
                ret = _process_request_storage(pr);
            }
            break;
        case 7:
            if (strncmp(cm, "replace", 7) == 0) {
                cmd = CMD_REPLACE;
                ret = _process_request_storage(pr);
            } else if (strncmp(cm, "prepend", 7) == 0) {
                cmd = CMD_PREPEND;
                ret = _process_request_storage(pr);
            } else if (strncmp(cm, "version", 7) == 0) {
                cmd = CMD_VERSION;
            }
            break;
    }

    // TODO: log more specific error code.
    if (cmd == -1 || ret != 0) {
        return -1;
    }
    // TODO: check if cmd unfound? need special code?
    pr->command = cmd;
    pr->cmd_type = type;

    return 0;
}

// FIXME: any reason to pass in command/cmdlen separately?
static mcp_request_t *mcp_new_request(lua_State *L, mcp_parser_t *pr, const char *command, size_t cmdlen) {
    // reserving an upvalue for key.
    mcp_request_t *rq = lua_newuserdatauv(L, sizeof(mcp_request_t), 1);
    memset(rq, 0, sizeof(*rq));
    memcpy(&rq->pr, pr, sizeof(*pr));

    // TODO: check rq->request and lua-fail properly
    char *request_copy = malloc(cmdlen);
    memcpy(request_copy, command, cmdlen);
    rq->pr.request = request_copy;
    rq->pr.reqlen = cmdlen;

    luaL_getmetatable(L, "mcp.request");
    lua_setmetatable(L, -2);

    // at this point we should know if we have to bounce through _nread to
    // get item data or not.
    return rq;
}

// second argument is optional, for building set requests.
// TODO: append the \r\n for the VAL?
static int mcplib_request(lua_State *L) {
    size_t len = 0;
    size_t vlen = 0;
    mcp_parser_t pr = {0};
    const char *cmd = luaL_checklstring(L, 1, &len);
    const char *val = luaL_optlstring(L, 2, NULL, &vlen);

    // FIXME: if we inline the userdata we can avoid memcpy'ing the parser
    // structure from the stack? but causes some code duplication.
    if (process_request(&pr, cmd, len) != 0) {
        proxy_lua_error(L, "failed to parse request");
        return 0;
    }
    mcp_request_t *rq = mcp_new_request(L, &pr, cmd, len);

    if (val != NULL) {
        rq->pr.vlen = vlen;
        rq->pr.vbuf = malloc(vlen);
        // TODO: check malloc failure.
        memcpy(rq->pr.vbuf, val, vlen);
    }

    // rq is now created, parsed, and on the stack.
    if (rq == NULL) {
        // TODO: lua error.
    }
    return 1;
}

// TODO: trace lua to confirm keeping the string in the uservalue ensures we
// don't create it multiple times if lua asks for it in a loop.
static int mcplib_request_key(lua_State *L) {
    mcp_request_t *rq = luaL_checkudata(L, -1, "mcp.request");

    if (!rq->lua_key) {
        rq->lua_key = true;
        lua_pushlstring(L, MCP_PARSER_KEY(rq->pr), rq->pr.klen);
        lua_pushvalue(L, -1); // push an extra copy to gobble.
        lua_setiuservalue(L, -3, 1);
        // TODO: push nil if no key parsed.
    } else{
        // FIXME: ensure != LUA_TNONE?
        lua_getiuservalue(L, -1, 1);
    }
    return 1;
}

static int mcplib_request_command(lua_State *L) {
    mcp_request_t *rq = luaL_checkudata(L, -1, "mcp.request");
    lua_pushinteger(L, rq->pr.command);
    return 1;
}

static int mcplib_request_gc(lua_State *L) {
    mcp_request_t *rq = luaL_checkudata(L, -1, "mcp.request");
    if (rq->pr.request != NULL) {
        // FIXME: discarding const here, fixing an error around free.
        // Can probably do something cleaner, so leaving a note here.
        // Think the answer is to move *request to mcp_r_t and only handle
        // const within the parser code.
        free((char *)rq->pr.request);
    }
    // FIXME: during nread c->item is the malloc'ed buffer. not yet put into
    // rq->buf - is this properly freed if the connection dies before
    // complete_nread?
    if (rq->pr.vbuf != NULL) {
        free(rq->pr.vbuf);
    }
    return 0;
}

// TODO: check what lua does when it calls a function with a string argument
// stored from a table/similar (ie; the prefix check code).
// If it's not copying anything, we can add request-side functions to do most
// forms of matching and avoid copying the key to lua space.

/*** END REQUET PARSER AND OBJECT ***/

/*** START jump consistent hash library ***/
// TODO: easy candidate for splitting to another .c, but I want this built in
// instead of as a .so so make sure it's linked directly.

typedef struct {
    struct proxy_hash_caller phc; // passed back to proxy API
    uint64_t seed;
    unsigned int buckets;
} mcplib_jump_hash_t;

static uint32_t mcplib_jump_hash_get_server(const void *key, size_t len, void *ctx) {
    mcplib_jump_hash_t *jh = ctx;

    uint64_t hash = XXH3_64bits_withSeed(key, len, jh->seed);

    int64_t b = -1, j = 0;
    while (j < jh->buckets) {
        b = j;
        hash = hash * 2862933555777941757ULL + 1;
        j = (b + 1) * ((double)(1LL << 31) / (double)((hash >> 33) + 1));
    }
    return b+1; // FIXME: do the -1 just for ketama and remove from internal code?
}

// stack = [pool, option]
static int mcplib_jump_hash_new(lua_State *L) {
    uint64_t seed = 0;
    const char *seedstr = NULL;
    size_t seedlen = 0;

    luaL_checktype(L, 1, LUA_TTABLE);
    lua_Unsigned buckets = lua_rawlen(L, 1);

    int argc = lua_gettop(L);
    if (argc > 1) {
        // options supplied. to be specified as a table.
        // { seed = "foo" }
        luaL_checktype(L, 2, LUA_TTABLE);
        // FIXME: adjust so we ensure/error on this being a string?
        if (lua_getfield(L, 2, "seed") != LUA_TNIL) {
            seedstr = lua_tolstring(L, -1, &seedlen);
            seed = XXH3_64bits(seedstr, seedlen);
        } else {
            dump_stack(L);
        }
        lua_pop(L, 1);
    }

    mcplib_jump_hash_t *jh = lua_newuserdatauv(L, sizeof(mcplib_jump_hash_t), 0);
    // TODO: check jh.

    // don't need to loop through the table at all, just need its length.
    // could optimize startup time by adding hints to the module for how to
    // format pool (ie; just a total count or the full table)
    jh->seed = seed;
    jh->buckets = buckets;
    jh->phc.ctx = jh;
    jh->phc.selector_func = mcplib_jump_hash_get_server;

    lua_pushlightuserdata(L, &jh->phc);

    // - return [UD, lightuserdata]
    return 2;
}

static int mcplib_open_jump_hash(lua_State *L) {
    const struct luaL_Reg jump_f[] = {
        {"new", mcplib_jump_hash_new},
        {NULL, NULL},
    };

    luaL_newlib(L, jump_f);

    return 1;
}

/*** END jump consistent hash library ***/

// Creates and returns the top level "mcp" module
int proxy_register_libs(LIBEVENT_THREAD *t, void *ctx) {
    lua_State *L = ctx;

    // TODO: stash into a table with weak references?
    // then if no pools/code has references still, can ditch?
    // TODO: __gc
    const struct luaL_Reg mcplib_backend_m[] = {
        {"set", NULL},
        {NULL, NULL}
    };

    const struct luaL_Reg mcplib_request_m[] = {
        {"command", mcplib_request_command},
        {"key", mcplib_request_key},
        {"__tostring", NULL},
        {"__gc", mcplib_request_gc},
        {NULL, NULL}
    };

    const struct luaL_Reg mcplib_response_m[] = {
        {"ok", mcplib_response_ok},
        {"__gc", mcplib_response_gc},
        {NULL, NULL}
    };

    // TODO: __gc
    const struct luaL_Reg mcplib_hash_selector_m[] = {
        {"__call", mcplib_hash_selector_call},
        {NULL, NULL}
    };

    const struct luaL_Reg mcplib_f [] = {
        {"hash_selector", mcplib_hash_selector},
        {"backend", mcplib_backend},
        {"request", mcplib_request},
        {"attach", mcplib_attach},
        {NULL, NULL}
    };

    // TODO: function + loop.
    luaL_newmetatable(L, "mcp.backend");
    lua_pushvalue(L, -1); // duplicate metatable.
    lua_setfield(L, -2, "__index"); // mt.__index = mt
    luaL_setfuncs(L, mcplib_backend_m, 0); // register methods
    lua_pop(L, 1);

    luaL_newmetatable(L, "mcp.request");
    lua_pushvalue(L, -1); // duplicate metatable.
    lua_setfield(L, -2, "__index"); // mt.__index = mt
    luaL_setfuncs(L, mcplib_request_m, 0); // register methods
    lua_pop(L, 1);

    luaL_newmetatable(L, "mcp.response");
    lua_pushvalue(L, -1); // duplicate metatable.
    lua_setfield(L, -2, "__index"); // mt.__index = mt
    luaL_setfuncs(L, mcplib_response_m, 0); // register methods
    lua_pop(L, 1);

    luaL_newmetatable(L, "mcp.hash_selector");
    lua_pushvalue(L, -1); // duplicate metatable.
    lua_setfield(L, -2, "__index"); // mt.__index = mt
    luaL_setfuncs(L, mcplib_hash_selector_m, 0); // register methods
    lua_pop(L, 1); // drop the hash selector metatable

    // create main library table.
    //luaL_newlib(L, mcplib_f);
    // TODO: luaL_newlibtable() just pre-allocs the exact number of things
    // here.
    // can replace with createtable and add the num. of the constant
    // definitions.
    luaL_newlibtable(L, mcplib_f);
    proxy_register_defines(L);

    // hash function for selectors.
    // have to wrap the function in a struct because function pointers aren't
    // pointer pointers :)
    mcplib_open_jump_hash(L);
    lua_setfield(L, -2, "hash_jump");
    // FIXME: remove this once multi-probe is in, use that as default instead.
    lua_pushlightuserdata(L, &mcplib_hashfunc_murmur3);
    lua_setfield(L, -2, "hash_murmur3");

    lua_pushlightuserdata(L, (void *)t); // upvalue for original thread
    lua_newtable(L); // upvalue for mcp.attach() table.

    luaL_setfuncs(L, mcplib_f, 2); // 2 upvalues.

    lua_setglobal(L, "mcp"); // set the lib table to mcp global.
    return 1;
}
