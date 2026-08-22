type Connection = Deno.Conn;

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

const HEADER_INCOMPLETE = Symbol("HEADER_INCOMPLETE");

export default {
    async fetch(request: Request, _env: unknown, _ctx: unknown): Promise<Response> {
        const userAgent = request.headers.get("user-agent");
        console.log(`[HTTP] URL: ${request.url} | UA: ${userAgent}`);

        if (request.headers.get("Upgrade") !== "websocket") {
            return new Response("Not Found", { status: 404 });
        }

        const uuid = Deno.env.get("UUID") ?? "";
        const { socket, response } = Deno.upgradeWebSocket(request);

        handleWebSocket(socket, uuid);
        return response;
    },
};

function handleWebSocket(webSocket: WebSocket, uuid: string): void {
    webSocket.binaryType = "arraybuffer";

    let processingChain = Promise.resolve();

    webSocket.addEventListener("message", (event: MessageEvent<ArrayBuffer>) => {
        processingChain = processingChain
            .then(() => processIncomingData(event.data))
            .catch(handleError);
    });

    webSocket.addEventListener("error", handleError);
    webSocket.addEventListener("close", cleanup);

    let headerBuffer = new Uint8Array(0);
    let isHeaderParsed = false;

    let remoteConnection: Connection | null = null;
    let remoteWriter: WritableStreamDefaultWriter<Uint8Array> | null = null;

    async function processIncomingData(data: ArrayBuffer): Promise<void> {
        const buffer = new Uint8Array(data);

        if (isHeaderParsed) {
            if (!remoteWriter) return;
            await remoteWriter.write(buffer);
            return;
        }

        const combined = new Uint8Array(headerBuffer.length + buffer.length);
        combined.set(headerBuffer, 0);
        combined.set(buffer, headerBuffer.length);
        headerBuffer = combined;

        const parseResult = parseHeader(headerBuffer, uuid);
        if (parseResult === HEADER_INCOMPLETE) return;

        const { version, port, hostname, payload } = parseResult;
        isHeaderParsed = true;
        console.log(`[VLS] Host: ${hostname}:${port} | Version: ${version}`);

        remoteConnection = await Deno.connect({ hostname, port });
        console.log(`[TCP] Connected to ${hostname}:${port}`);

        remoteWriter = remoteConnection.writable.getWriter();
        await remoteWriter.write(payload);

        const responseHeader = new Uint8Array([version, 0]);
        pipeRemoteToWebSocket(remoteConnection, webSocket, responseHeader);
    }

    async function handleError(error: unknown): Promise<void> {
        let message: string;

        if (error instanceof ErrorEvent) {
            message = error.error?.message || error.message;
        } else if (error instanceof Error) {
            message = error.message;
        } else {
            message = String(error);
        }

        console.log(`[Error] ${message}`);
        await cleanup();
    }

    async function cleanup(): Promise<void> {
        if (remoteWriter) {
            try {
                await remoteWriter.close();
            } catch {}
            try {
                remoteWriter.releaseLock();
            } catch {}
            remoteWriter = null;
        }
        if (remoteConnection) {
            try {
                remoteConnection.close();
            } catch {}
            remoteConnection = null;
        }
        try {
            webSocket.close();
        } catch {}
    }
}

async function pipeRemoteToWebSocket(
    remoteConnection: Connection,
    webSocket: WebSocket,
    responseHeader: Uint8Array,
): Promise<void> {
    const remoteReader = remoteConnection.readable.getReader();
    let isHeaderSent = false;

    try {
        while (true) {
            const { done, value } = await remoteReader.read();
            if (done) break;
            if (!value || value.byteLength === 0) continue;

            if (isHeaderSent) {
                webSocket.send(value);
                continue;
            }

            const combined = new Uint8Array(responseHeader.length + value.byteLength);
            combined.set(responseHeader, 0);
            combined.set(value, responseHeader.length);

            webSocket.send(combined);
            isHeaderSent = true;
        }
    } catch {
    } finally {
        try {
            await remoteReader.cancel();
        } catch {}
        try {
            remoteReader.releaseLock();
        } catch {}
        try {
            remoteConnection.close();
        } catch {}
        try {
            webSocket.close();
        } catch {}
    }
}

function parseHeader(buffer: Uint8Array, uuid: string): Header | typeof HEADER_INCOMPLETE {
    const VERSION_INDEX = 0;
    const UUID_INDEX = 1;
    const ADDONS_LENGTH_INDEX = 17;
    const ADDONS_INDEX = 18;

    const COMMAND_LENGTH = 1;
    const PORT_LENGTH = 2;
    const ADDRESS_TYPE_LENGTH = 1;

    const COMMAND_TCP = 1;

    if (buffer.byteLength < ADDONS_INDEX) {
        return HEADER_INCOMPLETE;
    }

    const version = buffer[VERSION_INDEX];
    if (!verifyUuidAt(buffer, UUID_INDEX, uuid)) {
        throw new Error("Invalid UUID");
    }

    const addonsLength = buffer[ADDONS_LENGTH_INDEX];
    const commandIndex = ADDONS_INDEX + addonsLength;

    if (buffer.byteLength < commandIndex) {
        return HEADER_INCOMPLETE;
    }

    const portIndex = commandIndex + COMMAND_LENGTH;
    const addressTypeIndex = portIndex + PORT_LENGTH;
    const addressIndex = addressTypeIndex + ADDRESS_TYPE_LENGTH;

    if (buffer.byteLength < addressIndex) {
        return HEADER_INCOMPLETE;
    }

    const command = buffer[commandIndex];
    if (command !== COMMAND_TCP) {
        throw new Error(`Invalid command code: ${command}`);
    }

    const port = (buffer[portIndex] << 8) | buffer[portIndex + 1];
    const addressType = buffer[addressTypeIndex];

    const parseResult = parseAddressAt(buffer, addressIndex, addressType);
    if (parseResult === HEADER_INCOMPLETE) return parseResult;

    const { hostname, nextIndex } = parseResult;
    const payload = buffer.subarray(nextIndex);

    return { version, port, hostname, payload };
}

function verifyUuidAt(buffer: Uint8Array, index: number, uuidString: string): boolean {
    const cleaned = uuidString.replace(/-/g, "");
    const uuidBytes = new Uint8Array(16);

    for (let i = 0; i < 16; i++) {
        uuidBytes[i] = parseInt(cleaned.substring(i * 2, i * 2 + 2), 16);
    }

    for (let i = 0; i < 16; i++) {
        if (buffer[index + i] !== uuidBytes[i]) return false;
    }

    return true;
}

function parseAddressAt(
    buffer: Uint8Array,
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

            if (buffer.byteLength < nextIndex) {
                return HEADER_INCOMPLETE;
            }

            const hostname = buffer.subarray(index, nextIndex).join(".");
            return { hostname, nextIndex };
        }
        case ADDRESS_TYPE_DOMAIN: {
            const domainIndex = index + 1;

            if (buffer.byteLength < domainIndex) {
                return HEADER_INCOMPLETE;
            }

            const domainLength = buffer[index];
            const nextIndex = domainIndex + domainLength;

            if (buffer.byteLength < nextIndex) {
                return HEADER_INCOMPLETE;
            }

            const domainBuffer = buffer.subarray(domainIndex, nextIndex);
            const hostname = new TextDecoder().decode(domainBuffer);

            return { hostname, nextIndex };
        }
        case ADDRESS_TYPE_IPV6: {
            const nextIndex = index + IPV6_LENGTH;

            if (buffer.byteLength < nextIndex) {
                return HEADER_INCOMPLETE;
            }

            const segments: string[] = [];
            for (let i = 0; i < IPV6_SEGMENT_COUNT; i++) {
                const segmentIndex = index + i * IPV6_SEGMENT_LENGTH;
                const segmentValue = (buffer[segmentIndex] << 8) | buffer[segmentIndex + 1];
                segments.push(segmentValue.toString(16));
            }

            const hostname = segments.join(":");
            return { hostname, nextIndex };
        }
        default: {
            throw new Error(`Invalid address type: ${addressType}`);
        }
    }
}
