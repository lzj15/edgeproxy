import { Hono, Context } from "hono";
import { env } from "hono/adapter";
import { WSContext } from "hono/ws";

let upgradeWebSocket: any;
let connect: any;

const isCloudflare = typeof WebSocketPair !== "undefined";

if (isCloudflare) {
    const cfWorkers = await import("@hono/cloudflare-workers");
    const cfSockets = await import("cloudflare:sockets");

    upgradeWebSocket = cfWorkers.upgradeWebSocket;
    connect = cfSockets.connect;
} else {
    const nodeServer = await import("@hono/node-server");
    const nodeSocket = await import("@arrowood.dev/socket");

    upgradeWebSocket = nodeServer.upgradeWebSocket;
    connect = nodeSocket.connect;
}

type Session = {
    uuid: string;
    ws: WSContext | null;
    wsMessageQueue: Promise<void>;
    pendingHeader: Uint8Array;
    tcpSocket: Socket | null;
    tcpWriter: WritableStreamDefaultWriter<Uint8Array> | null;
};

type Header = {
    version: number;
    port: number;
    hostname: string;
    payload: Uint8Array;
};

type Address = {
    hostname: string;
    nextIndex: number;
};

const HEADER_INCOMPLETE: unique symbol = Symbol("HEADER_INCOMPLETE");

const app = new Hono();

if (!isCloudflare) {
    const { WebSocketServer } = await import("ws");
    const { serve } = await import("@hono/node-server");

    const wss = new WebSocketServer({ noServer: true });

    serve({
        fetch: app.fetch,
        websocket: { server: wss },
    });
}

export default {
    fetch(request: Request, env: unknown, ctx: ExecutionContext) {
        return app.fetch(request, env, ctx);
    },
};

app.use("*", async (c, next) => {
    console.log(`[HTTP] URL: ${c.req.url} | UA: ${c.req.header("user-agent")}`);
    await next();
});

app.get("/ws", upgradeWebSocket(createWsHandler));

function createWsHandler(c: Context) {
    const { UUID } = env<{ UUID: string }>(c);

    const session: Session = {
        uuid: UUID,
        ws: null,
        wsMessageQueue: Promise.resolve(),
        pendingHeader: new Uint8Array(0),
        tcpSocket: null,
        tcpWriter: null,
    };

    async function toUint8Array(data: Blob | ArrayBuffer): Promise<Uint8Array> {
        if (data instanceof Blob) {
            return await data.bytes();
        }
        return new Uint8Array(data);
    }

    return {
        onMessage(event: MessageEvent, ws: WSContext) {
            session.ws = ws;
            session.wsMessageQueue = session.wsMessageQueue
                .then(async () => {
                    const bytes = await toUint8Array(event.data);
                    return processWsData(bytes, ws, session);
                })
                .catch((error) => {
                    handleError(session, error);
                });
        },
        onClose(event: CloseEvent) {
            console.log(`[WS] Closed with code ${event.code}`);
            cleanup(session);
        },
        onError(event: Event, ws: WSContext) {
            handleError(session, new Error("WebSocket error"));
        },
    };
}

async function processWsData(
    bytes: Uint8Array,
    ws: WSContext,
    session: Session,
): Promise<void> {
    if (!session.ws) return;

    if (session.tcpWriter) {
        await session.tcpWriter.write(bytes);
        return;
    }

    if (!session.uuid) {
        throw new Error("UUID is not configured");
    }

    function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
        const result = new Uint8Array(a.length + b.length);
        result.set(a, 0);
        result.set(b, a.byteLength);
        return result;
    }

    session.pendingHeader = concat(session.pendingHeader, bytes);
    const parseResult = parseHeader(session.pendingHeader, session.uuid);

    if (parseResult === HEADER_INCOMPLETE) return;
    session.pendingHeader = new Uint8Array(0);

    const { version, port, hostname, payload } = parseResult;
    console.log(`[VLS] Host: ${hostname}:${port} | Version: ${version}`);

    const tcpSocket = connect({ hostname, port });
    await tcpSocket.opened;

    const tcpWriter = tcpSocket.writable.getWriter();
    await tcpWriter.write(payload);

    session.tcpSocket = tcpSocket;
    session.tcpWriter = tcpWriter;

    ws.send(new Uint8Array([version, 0]));
    processTcpData(tcpSocket, ws, session);

    console.log(`[TCP] Connected to ${hostname}:${port}`);
}

async function processTcpData(
    tcpSocket: Socket,
    ws: WSContext,
    session: Session,
): Promise<void> {
    const tcpReader = tcpSocket.readable.getReader();

    try {
        while (true) {
            const { done, value } = await tcpReader.read();
            if (done) break;
            ws.send(value);
        }
    } catch (error) {
        handleError(session, error);
    } finally {
        try {
            tcpReader.releaseLock();
        } catch {}

        cleanup(session);
    }
}

function handleError(session: Session, error: unknown): void {
    let message: string;

    if (error instanceof Error) {
        message = error.message;
    } else {
        message = String(error);
    }

    console.log(`[Error] ${message}`);
    cleanup(session);
}

function cleanup(session: Session): void {
    if (session.tcpWriter) {
        try {
            session.tcpWriter.releaseLock();
        } catch {}
        session.tcpWriter = null;
    }

    if (session.tcpSocket) {
        try {
            session.tcpSocket.close();
        } catch {}
        session.tcpSocket = null;
    }

    if (session.ws) {
        try {
            session.ws.close();
        } catch {}
        session.ws = null;
    }
}

function parseHeader(
    bytes: Uint8Array,
    uuid: string,
): Header | typeof HEADER_INCOMPLETE {
    const VERSION_INDEX = 0;
    const UUID_INDEX = 1;
    const ADDONS_LENGTH_INDEX = 17;
    const ADDONS_INDEX = 18;

    const COMMAND_LENGTH = 1;
    const PORT_LENGTH = 2;
    const ADDRESS_TYPE_LENGTH = 1;

    const COMMAND_TCP = 1;

    if (bytes.length < ADDONS_INDEX) {
        return HEADER_INCOMPLETE;
    }

    const version = bytes[VERSION_INDEX];
    if (!verifyUuidAt(bytes, UUID_INDEX, uuid)) {
        throw new Error("Invalid UUID");
    }

    const addonsLength = bytes[ADDONS_LENGTH_INDEX];
    const commandIndex = ADDONS_INDEX + addonsLength;

    if (bytes.byteLength < commandIndex + COMMAND_LENGTH) {
        return HEADER_INCOMPLETE;
    }

    const portIndex = commandIndex + COMMAND_LENGTH;
    const addressTypeIndex = portIndex + PORT_LENGTH;
    const addressIndex = addressTypeIndex + ADDRESS_TYPE_LENGTH;

    if (bytes.byteLength < addressIndex) {
        return HEADER_INCOMPLETE;
    }

    const command = bytes[commandIndex];
    if (command !== COMMAND_TCP) {
        throw new Error(`Invalid command code: ${command}`);
    }

    const port = (bytes[portIndex] << 8) | bytes[portIndex + 1];
    const addressType = bytes[addressTypeIndex];

    const parseResult = parseAddressAt(bytes, addressIndex, addressType);
    if (parseResult === HEADER_INCOMPLETE) return parseResult;

    const { hostname, nextIndex } = parseResult;
    const payload = bytes.subarray(nextIndex);

    return { version, port, hostname, payload };
}

function verifyUuidAt(
    bytes: Uint8Array,
    index: number,
    uuidString: string,
): boolean {
    const cleaned = uuidString.replace(/-/g, "");
    if (cleaned.length !== 32) return false;

    for (let i = 0; i < 16; i++) {
        const byte = parseInt(cleaned.substring(i * 2, i * 2 + 2), 16);
        if (byte !== bytes[index + i]) return false;
    }

    return true;
}

function parseAddressAt(
    bytes: Uint8Array,
    index: number,
    addressType: number,
): Address | typeof HEADER_INCOMPLETE {
    const ADDRESS_TYPE_IPV4 = 1;
    const ADDRESS_TYPE_DOMAIN = 2;
    const ADDRESS_TYPE_IPV6 = 3;

    const IPV4_LENGTH = 4;
    const IPV6_LENGTH = 16;
    const IPV6_SEGMENT_COUNT = 8;
    const IPV6_SEGMENT_LENGTH = 2;

    switch (addressType) {
        case ADDRESS_TYPE_IPV4: {
            const nextIndex = index + IPV4_LENGTH;

            if (bytes.length < nextIndex) {
                return HEADER_INCOMPLETE;
            }

            const hostname = bytes.subarray(index, nextIndex).join(".");
            return { hostname, nextIndex };
        }
        case ADDRESS_TYPE_DOMAIN: {
            const domainIndex = index + 1;

            if (bytes.length < domainIndex) {
                return HEADER_INCOMPLETE;
            }

            const domainLength = bytes[index];
            const nextIndex = domainIndex + domainLength;

            if (bytes.length < nextIndex) {
                return HEADER_INCOMPLETE;
            }

            const domainBytes = bytes.subarray(domainIndex, nextIndex);
            const hostname = new TextDecoder().decode(domainBytes);

            return { hostname, nextIndex };
        }
        case ADDRESS_TYPE_IPV6: {
            const nextIndex = index + IPV6_LENGTH;

            if (bytes.length < nextIndex) {
                return HEADER_INCOMPLETE;
            }

            const segments: string[] = [];
            for (let i = 0; i < IPV6_SEGMENT_COUNT; i++) {
                const segmentIndex = index + i * IPV6_SEGMENT_LENGTH;
                const segmentValue =
                    (bytes[segmentIndex] << 8) | bytes[segmentIndex + 1];

                segments.push(segmentValue.toString(16));
            }

            const hostname = segments.join(":");
            return { hostname, nextIndex };
        }
        default: {
            throw new Error(`Invalid address type code: ${addressType}`);
        }
    }
}
