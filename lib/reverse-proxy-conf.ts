// lib/reverse-proxy-conf.ts
import { ensureDir } from "@std/fs";
import { normalize as normalizePath } from "@std/path";
import { taggedProcesses } from "./spawn.ts";

function safeFileName(s: string) {
  return s.replaceAll(/[^A-Za-z0-9._-]/g, "_");
}

async function writeTextAtomic(path: string, content: string) {
  const p = normalizePath(path).replaceAll("\\", "/");
  const dir = p.slice(0, Math.max(0, p.lastIndexOf("/")));
  if (dir) await ensureDir(dir);
  const tmp = `${p}.tmp`;
  await Deno.writeTextFile(tmp, content);
  await Deno.rename(tmp, p);
}

function fnv1a32Hex(s: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(16).padStart(8, "0");
}

function trimTrailingSlashes(s: string): string {
  return s.replaceAll(/\/+$/g, "");
}

function ensureTrailingSlash(s: string): string {
  return s.endsWith("/") ? s : `${s}/`;
}

function escapeForNginxRegexPrefix(pathPrefixWithSlash: string): string {
  return pathPrefixWithSlash.replaceAll("/", "\\/");
}

function parseEntryPointsCsv(s: string): string[] {
  return s.split(",").map((x) => x.trim()).filter((x) => x.length > 0);
}

export type ProxyConfOverrides = Readonly<{
  nginx?: Readonly<{
    locationPrefix?: string;
    serverName?: string;
    listen?: string;
    stripPrefix?: boolean;
    extra?: string;
  }>;
  traefik?: Readonly<{
    locationPrefix?: string;
    entrypoints?: string; // CSV
    rule?: string;
    stripPrefix?: boolean;
    extra?: string;
  }>;
  nginxProxyManager?: Readonly<{
    locationPrefix?: string;
    upstreamScheme?: string;
    upstreamHost?: string;
  }>;
}>;

type SpawnedState = Awaited<ReturnType<typeof taggedProcesses>> extends
  AsyncGenerator<infer S> ? S : never;

function stateId(s: SpawnedState): string {
  return s.serviceId ?? "stateId?";
}

function stateKind(s: SpawnedState): string {
  return s.context?.service.kind ?? "stateKind?";
}

function stateDbPath(s: SpawnedState): string {
  return (s.context?.supplier as { location?: string }).location ?? "";
}

function defaultLocationPrefixFromState(s: SpawnedState): string {
  const p = s.context?.service.proxyEndpointPrefix || "/";
  const norm = p.replaceAll("\\", "/").replaceAll(/\/+/g, "/").trim();
  return ensureTrailingSlash(norm.startsWith("/") ? norm : `/${norm}`);
}

function upstreamFromState(s: SpawnedState): string {
  return s.context?.listen.baseUrl ?? "";
}

function nginxHeaderLine(name: string, value: string | number): string {
  // keep header names stable and explicit
  return `    proxy_set_header X-Truth-Yard-${name} "${String(value)}";\n`;
}

function buildNginxDbYardHeaders(args: {
  id: string;
  dbPath: string;
  kind: string;
  pid: number;
  upstream: string;
  proxyPrefix: string;
}): string {
  const { id, dbPath, kind, pid, upstream, proxyPrefix } = args;
  return (
    nginxHeaderLine("Id", id) +
    nginxHeaderLine("Db", dbPath) +
    nginxHeaderLine("Kind", kind) +
    nginxHeaderLine("Pid", pid) +
    nginxHeaderLine("Upstream", upstream) +
    nginxHeaderLine("ProxyPrefix", proxyPrefix)
  );
}

function yamlEscape(s: string): string {
  // simple + safe: always use double quotes and escape backslash + quote
  return s.replaceAll("\\", "\\\\").replaceAll('"', '\\"');
}

export function nginxReverseProxyConfFromState(
  s: SpawnedState,
  overrides: ProxyConfOverrides = {},
): string {
  const id = stateId(s);
  const kind = stateKind(s);
  const dbPath = stateDbPath(s);
  const upstream = upstreamFromState(s);

  const locationPrefix = overrides.nginx?.locationPrefix ??
    defaultLocationPrefixFromState(s);

  const serverName = overrides.nginx?.serverName ?? "_";
  const listen = overrides.nginx?.listen ?? "80";
  const stripPrefix = overrides.nginx?.stripPrefix ?? false;
  const extra = overrides.nginx?.extra ?? "";

  const name = safeFileName(id);
  const hash = fnv1a32Hex(id);

  const rawLp = ensureTrailingSlash(locationPrefix).replaceAll(/\/+/g, "/");
  const lp = (rawLp === "/") ? "/" : trimTrailingSlashes(rawLp);

  const rewriteLine = stripPrefix
    ? `    rewrite ^${escapeForNginxRegexPrefix(lp)}(.*)$ /$1 break;\n`
    : "";

  const extraBlock = extra.trim() ? `\n${extra.trimEnd()}\n` : "";

  const hdrs = buildNginxDbYardHeaders({
    id,
    dbPath,
    kind,
    pid: s.pid,
    upstream,
    proxyPrefix: rawLp,
  });

  return `# Truth Yard nginx reverse proxy (generated)
# id=${id}
# db=${dbPath}
# kind=${kind}
# pid=${s.pid}
# upstream=${upstream}
# proxyPrefix=${rawLp}

# Suggested include filename:
#   truth-yard.${name}.${hash}.conf

server {
  listen ${listen};
  server_name ${serverName};

  location ${lp} {
${rewriteLine}    proxy_pass ${upstream};
    proxy_http_version 1.1;

    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;

${hdrs}  }${extraBlock}}
`;
}

function nginxLocationBlockFromState(
  s: SpawnedState,
  overrides: ProxyConfOverrides = {},
): string {
  const id = stateId(s);
  const kind = stateKind(s);
  const dbPath = stateDbPath(s);
  const upstream = upstreamFromState(s);

  const locationPrefix = overrides.nginx?.locationPrefix ??
    defaultLocationPrefixFromState(s);

  const stripPrefix = overrides.nginx?.stripPrefix ?? false;

  const name = safeFileName(id);
  const hash = fnv1a32Hex(id);

  const rawLp = ensureTrailingSlash(locationPrefix).replaceAll(/\/+/g, "/");
  const lp = (rawLp === "/") ? "/" : trimTrailingSlashes(rawLp);

  const rewriteLine = stripPrefix
    ? `    rewrite ^${escapeForNginxRegexPrefix(lp)}(.*)$ /$1 break;\n`
    : "";

  const hdrs = buildNginxDbYardHeaders({
    id,
    dbPath,
    kind,
    pid: s.pid,
    upstream,
    proxyPrefix: rawLp,
  });

  return `  # Truth Yard nginx reverse proxy (generated)
  # id=${id}
  # db=${dbPath}
  # kind=${kind}
  # pid=${s.pid}
  # upstream=${upstream}
  # proxyPrefix=${rawLp}

  # Suggested include filename:
  #   truth-yard.${name}.${hash}.conf

  location ${lp} {
${rewriteLine}    proxy_pass ${upstream};
    proxy_http_version 1.1;

    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;

${hdrs}  }
  `;
}

export function nginxBundledReverseProxyConf(
  states: SpawnedState[],
  overrides: ProxyConfOverrides = {},
): string {
  if (states.length === 0) return "";

  const serverName = overrides.nginx?.serverName ?? "_";
  const listen = overrides.nginx?.listen ?? "80";
  const extra = overrides.nginx?.extra ?? "";
  const extraBlock = extra.trim() ? `\n${extra.trimEnd()}\n` : "";

  const locationBlocks = states
    .map((s) => nginxLocationBlockFromState(s, overrides))
    .join("\n");

  return `server {
  listen ${listen};
  server_name ${serverName};

  root /var/www/html;
  index index.html;

  location / {
    try_files $uri $uri/ =404;
  }

${locationBlocks}
}
${extraBlock}`;
}

export function traefikReverseProxyConfFromState(
  s: SpawnedState,
  overrides: ProxyConfOverrides = {},
): string {
  const id = stateId(s);
  const kind = stateKind(s);
  const dbPath = stateDbPath(s);

  const locationPrefix = overrides.traefik?.locationPrefix ??
    defaultLocationPrefixFromState(s);

  const lpNoTrail = trimTrailingSlashes(locationPrefix) ||
    trimTrailingSlashes(defaultLocationPrefixFromState(s));

  const url = upstreamFromState(s);

  const entrypointsRaw = overrides.traefik?.entrypoints ?? "web";
  const entryPoints = parseEntryPointsCsv(entrypointsRaw);
  const entryPointsYaml = entryPoints.length ? entryPoints.join(", ") : "web";

  const defaultRule = `PathPrefix(\`${lpNoTrail}/\`)`;
  const rule = overrides.traefik?.rule ?? defaultRule;

  const stripPrefix = overrides.traefik?.stripPrefix ?? false;
  const extraYaml = overrides.traefik?.extra ?? "";

  const name = safeFileName(id);
  const hash = fnv1a32Hex(id);

  const routerName = `truth-yard-${name}-${hash}`;
  const serviceName = `svc-${name}-${hash}`;
  const mwStripName = `mw-strip-${name}-${hash}`;
  const mwHdrName = `mw-hdr-${name}-${hash}`;

  const mwBlock = `
  middlewares:
    ${mwHdrName}:
      headers:
        customRequestHeaders:
          X-Truth-Yard-Id: "${yamlEscape(id)}"
          X-Truth-Yard-Db: "${yamlEscape(dbPath)}"
          X-Truth-Yard-Kind: "${yamlEscape(kind)}"
          X-Truth-Yard-Pid: "${yamlEscape(String(s.pid))}"
          X-Truth-Yard-Upstream: "${yamlEscape(url)}"
          X-Truth-Yard-ProxyPrefix: "${yamlEscape(lpNoTrail + "/")}"${
    stripPrefix
      ? `
    ${mwStripName}:
      stripPrefix:
        prefixes:
          - "${yamlEscape(lpNoTrail)}"`
      : ""
  }
`;

  const middlewares = stripPrefix
    ? `[${mwHdrName}, ${mwStripName}]`
    : `[${mwHdrName}]`;

  const extraBlock = extraYaml.trim() ? `\n${extraYaml.trimEnd()}\n` : "";

  return `# Truth Yard traefik dynamic config (generated)
# id=${id}
# db=${dbPath}
# kind=${kind}
# pid=${s.pid}
# upstream=${url}
# proxyPrefix=${lpNoTrail}/
http:
  routers:
    ${routerName}:
      rule: ${rule}
      entryPoints: [${entryPointsYaml}]
      service: ${serviceName}
      middlewares: ${middlewares}

  services:
    ${serviceName}:
      loadBalancer:
        passHostHeader: true
        servers:
          - url: "${yamlEscape(url)}"
${mwBlock}${extraBlock}`;
}

export async function generateReverseProxyConfsFromSpawnedStates(args: {
  nginxConfHome?: string;
  traefikConfHome?: string;
  verbose?: boolean;
  overrides?: ProxyConfOverrides;
}) {
  const overrides = args.overrides ?? {};

  const states: SpawnedState[] = [];
  for await (const s of taggedProcesses()) {
    states.push(s);
  }

  if (args.nginxConfHome) {
    const dir = normalizePath(args.nginxConfHome);
    await ensureDir(dir);

    for (const s of states) {
      const id = stateId(s);
      const fn = `truth-yard.${safeFileName(id)}.conf`;
      await writeTextAtomic(
        `${dir}/${fn}`,
        nginxReverseProxyConfFromState(s, overrides),
      );
    }

    const bundle = nginxBundledReverseProxyConf(states, overrides);
    await writeTextAtomic(`${dir}/truth-yard.generated.conf`, bundle);

    if (args.verbose) {
      console.log(
        `[spawned] wrote nginx conf(s) to: ${dir} (and truth-yard.generated.conf)`,
      );
    }
  }

  if (args.traefikConfHome) {
    const dir = normalizePath(args.traefikConfHome);
    await ensureDir(dir);

    for (const s of states) {
      const id = stateId(s);
      const fn = `truth-yard.${safeFileName(id)}.yaml`;
      await writeTextAtomic(
        `${dir}/${fn}`,
        traefikReverseProxyConfFromState(s, overrides),
      );
    }

    const bundle = states.map((s) =>
      traefikReverseProxyConfFromState(s, overrides)
    )
      .join("\n");
    await writeTextAtomic(`${dir}/truth-yard.generated.yaml`, bundle);

    if (args.verbose) {
      console.log(
        `[spawned] wrote traefik conf(s) to: ${dir} (and truth-yard.generated.yaml)`,
      );
    }
  }

  if (!args.nginxConfHome && !args.traefikConfHome) {
    console.log(nginxBundledReverseProxyConf(states, overrides));
  }
}

export function nginxProxyManagerJSON(
  states: SpawnedState[],
  overrides: ProxyConfOverrides = {},
): string {
  const locations = states.map((s) => {
    const id = stateId(s);
    const kind = stateKind(s);
    const dbPath = stateDbPath(s);
    const upstream = upstreamFromState(s);
    const upstreamUrl = new URL(upstream);
    const locationPrefix = overrides.nginxProxyManager?.locationPrefix ??
      defaultLocationPrefixFromState(s);
    const rawLp = ensureTrailingSlash(locationPrefix).replaceAll(/\/+/g, "/");
    const lp = (rawLp === "/") ? "/" : trimTrailingSlashes(rawLp);

    const forwardScheme = overrides.nginxProxyManager?.upstreamScheme ?? "http";
    const forwardHost = overrides.nginxProxyManager?.upstreamHost ?? "0.0.0.0";
    const forwardPort = parseInt(upstreamUrl.port) ||
      (upstreamUrl.protocol === "https:" ? 443 : 80);

    const advConfig = [
      ["Id", id],
      ["Db", dbPath],
      ["Kind", kind],
      ["Pid", s.pid],
      ["Upstream", `${forwardScheme}://${forwardHost}:${forwardPort}`],
      ["ProxyPrefix", rawLp],
    ].map(([name, value]) =>
      `proxy_set_header X-Truth-Yard-${name} "${String(value)}";`
    ).join("\n");

    return {
      path: lp,
      forward_scheme: forwardScheme,
      forward_host: forwardHost,
      forward_port: forwardPort,
      advanced_config: advConfig,
    };
  });

  return JSON.stringify({ locations }, null, 2);
}
