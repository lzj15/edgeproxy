const IS_DENO = typeof Deno !== "undefined";
const IS_WORKER = typeof WebSocketPair !== "undefined";
const IS_NODE = !IS_DENO && !IS_WORKER && typeof process !== "undefined";

const UUID = "27d54616-30a8-4935-87dc-3f7573ed1e03";

if (IS_NODE) {
    const http = await import("node:http");
    const { WebSocketServer } = await import("ws");

    const server = http.createServer((req, res) => {
        res.writeHead(200, { "Content-Type": "text/plain" });
        res.end("Hello World!");
    });

    const wss = new WebSocketServer({ noServer: true });

    server.on("upgrade", (request, socket, head) => {
        wss.handleUpgrade(request, socket, head, (ws) => {
            wss.emit("connection", ws, request);
        });
    });

    wss.on("connection", (webSocket) => {
        webSocket.binaryType = "arraybuffer";
        handleWebSocketConnection(webSocket, UUID);
    });

    const PORT = 443;
    server.listen(PORT, () => {
        console.log(`[HTTP] Listening on port ${PORT}`);
    });
}

export default {
    async fetch(request, env, ctx) {
        if (request.headers.get("Upgrade") !== "websocket") {
            return new Response("Hello World!", { status: 200 });
        }

        const userAgent = request.headers.get("user-agent");
        console.log(`[HTTP] URL: ${request.url} | UA: ${userAgent}`);

        if (IS_WORKER) {
            const webSocketPair = new WebSocketPair();
            const [clientSocket, serverSocket] = Object.values(webSocketPair);

            serverSocket.accept();
            serverSocket.binaryType = "arraybuffer";

            handleWebSocketConnection(serverSocket, UUID);

            return new Response(null, {
                status: 101,
                webSocket: clientSocket,
                headers: { "Sec-WebSocket-Extensions": "" },
            });
        }

        if (IS_DENO) {
            const { socket: webSocket, response } = Deno.upgradeWebSocket(
                request,
            );
            webSocket.binaryType = "arraybuffer";
            handleWebSocketConnection(webSocket, UUID);
            return response;
        }
    },
};

async function handleWebSocketConnection(webSocket, uuid) {
    let isHeaderParsed = false;
    let remoteSocket = null;
    let remoteStreamWriter = null;
    let processingQueueChain = Promise.resolve();

    setupWebSocketListeners(
        webSocket,
        (data) => {
            processingQueueChain = processingQueueChain
                .then(() => processIncomingChunk(data))
                .catch(handleError);
        },
        handleError,
        cleanup,
    );

    async function processIncomingChunk(chunk) {
        if (isHeaderParsed) {
            if (!remoteSocket) return;
            if (!remoteStreamWriter) {
                remoteStreamWriter = remoteSocket.writable.getWriter();
            }
            await remoteStreamWriter.write(new Uint8Array(chunk));
            return;
        }

        const parseResult = parseHeader(new Uint8Array(chunk), uuid);
        isHeaderParsed = true;

        if (parseResult.hasError) {
            throw new Error(parseResult.errorMessage);
        }

        const { version, port, hostname, payload } = parseResult;
        console.log(`[VLS] Host: ${hostname}:${port} | Version: ${version}`);

        remoteSocket = await connectSocket(hostname, port);
        console.log(`[TCP] Connected to ${hostname}:${port}`);

        const writer = remoteSocket.writable.getWriter();
        await writer.write(payload);
        writer.releaseLock();

        const responseHeader = new Uint8Array([version, 0]);
        pipeTcpStreamToWebSocket(remoteSocket, webSocket, responseHeader);
    }

    function handleError(error) {
        console.log(`[Error] ${error.message}`);
        cleanup();
    }

    function cleanup() {
        if (remoteStreamWriter) {
            try {
                remoteStreamWriter.close();
                remoteStreamWriter.releaseLock();
            } catch {}
            remoteStreamWriter = null;
        }
        if (remoteSocket) {
            try {
                remoteSocket.close();
            } catch {}
            remoteSocket = null;
        }
        try {
            webSocket.close();
        } catch {}
    }
}

function setupWebSocketListeners(webSocket, onMessage, onError, onClose) {
    if (IS_NODE) {
        webSocket.on("message", (data) => {
            onMessage(data instanceof ArrayBuffer ? data : data.buffer);
        });
        webSocket.on("error", onError);
        webSocket.on("close", onClose);
    } else {
        webSocket.addEventListener("message", (event) => onMessage(event.data));
        webSocket.addEventListener("error", onError);
        webSocket.addEventListener("close", onClose);
    }
}

async function connectSocket(hostname, port) {
    if (IS_WORKER) {
        const { connect } = await import("cloudflare:sockets");
        return connect({ hostname, port });
    }

    if (IS_DENO) {
        const conn = await Deno.connect({ hostname, port });
        return {
            readable: conn.readable,
            writable: conn.writable,
            close: () => {
                try {
                    conn.close();
                } catch {}
            },
        };
    }

    const net = await import("node:net");
    const { Readable, Writable } = await import("node:stream");

    const remoteSocket = net.connect({ host: hostname, port: port });

    await new Promise((resolve, reject) => {
        remoteSocket.once("connect", resolve);
        remoteSocket.once("error", reject);
    });

    return {
        readable: Readable.toWeb(remoteSocket),
        writable: Writable.toWeb(remoteSocket),
        close: () => remoteSocket.destroy(),
    };
}

async function pipeTcpStreamToWebSocket(
    remoteSocket,
    webSocket,
    responseHeader,
) {
    let pendingHeader = responseHeader;
    const streamReader = remoteSocket.readable.getReader();

    try {
        while (true) {
            const { done, value } = await streamReader.read();
            if (done) break;
            if (!value || value.byteLength === 0) continue;

            if (pendingHeader) {
                const combinedBuffer = new Uint8Array(
                    pendingHeader.length + value.byteLength,
                );
                combinedBuffer.set(pendingHeader, 0);
                combinedBuffer.set(value, pendingHeader.length);
                pendingHeader = null;
                webSocket.send(combinedBuffer);
            } else {
                webSocket.send(value);
            }
        }
    } catch {
    } finally {
        if (streamReader) {
            await streamReader.cancel().catch(() => {});
            streamReader.releaseLock();
        }
        try {
            remoteSocket.close();
            webSocket.close();
        } catch {}
    }
}

function parseHeader(bufferChunk, uuid) {
    const COMMAND_TCP = 1;

    const uint8Data = bufferChunk;
    const totalLength = uint8Data.byteLength;

    if (totalLength < 24) {
        return { hasError: true, errorMessage: "Invalid header data" };
    }

    const version = uint8Data[0];
    if (!verifyUuidMatch(uint8Data, 1, uuid)) {
        return { hasError: true, errorMessage: "Invalid UUID" };
    }

    const addonsLength = uint8Data[17];
    const commandIndex = 18 + addonsLength;

    if (totalLength < commandIndex + 4) {
        return { hasError: true, errorMessage: "Invalid header data" };
    }
    if (uint8Data[commandIndex] !== COMMAND_TCP) {
        return { hasError: true, errorMessage: "Invalid command code" };
    }

    const portIndex = commandIndex + 1;
    const view = new DataView(
        uint8Data.buffer,
        uint8Data.byteOffset,
        totalLength,
    );
    const port = view.getUint16(portIndex, false);

    const addressType = uint8Data[portIndex + 2];
    const addressValueIndex = portIndex + 3;

    const addressResult = parseAddress(
        uint8Data,
        addressType,
        addressValueIndex,
    );
    if (addressResult.hasError) return addressResult;

    return {
        hasError: false,
        version,
        port,
        hostname: addressResult.hostname,
        payload: uint8Data.subarray(addressResult.nextIndex),
    };
}

function verifyUuidMatch(buffer, byteOffset, uuidString) {
    const clean = uuidString.replace(/-/g, "");
    const uuidBytes = new Uint8Array(16);
    for (let i = 0; i < 16; i++) {
        uuidBytes[i] = parseInt(clean.substring(i * 2, i * 2 + 2), 16);
    }
    for (let i = 0; i < 16; i++) {
        if (buffer[byteOffset + i] !== uuidBytes[i]) return false;
    }
    return true;
}

function parseAddress(uint8Data, addressType, offset) {
    const ADDRESS_TYPES = { IPV4: 1, DOMAIN: 2, IPV6: 3 };

    const totalLength = uint8Data.byteLength;

    switch (addressType) {
        case ADDRESS_TYPES.IPV4: {
            if (totalLength < offset + 4) {
                return { hasError: true, errorMessage: "Invalid IPv4 length" };
            }
            return {
                hostname: uint8Data.subarray(offset, offset + 4).join("."),
                nextIndex: offset + 4,
            };
        }
        case ADDRESS_TYPES.DOMAIN: {
            if (totalLength < offset + 1) {
                return {
                    hasError: true,
                    errorMessage: "Invalid domain length",
                };
            }
            const domainLength = uint8Data[offset];
            const start = offset + 1;
            const end = start + domainLength;
            if (totalLength < end) {
                return { hasError: true, errorMessage: "Invalid domain data" };
            }
            return {
                hostname: new TextDecoder().decode(
                    uint8Data.subarray(start, end),
                ),
                nextIndex: end,
            };
        }
        case ADDRESS_TYPES.IPV6: {
            if (totalLength < offset + 16) {
                return { hasError: true, errorMessage: "Invalid IPv6 length" };
            }
            const segments = [];
            for (let i = 0; i < 8; i++) {
                const idx = offset + i * 2;
                segments.push(
                    ((uint8Data[idx] << 8) | uint8Data[idx + 1]).toString(16),
                );
            }
            return { hostname: segments.join(":"), nextIndex: offset + 16 };
        }
        default: {
            return {
                hasError: true,
                errorMessage: `Invalid address type: ${addressType}`,
            };
        }
    }
}
