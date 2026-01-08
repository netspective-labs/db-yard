// lib/reverse-proxy-conf.ts
import { normalize as normalizePath } from "@std/path";
import { ensureDir } from "@std/fs";

import { spawnedStates } from "./materialize.ts";

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
}>;

type SpawnedState = Awaited<ReturnType<typeof spawnedStates>> extends
  AsyncGenerator<infer S> ? S : never;

function stateId(s: SpawnedState): string {
  return s.context.service.id;
}

function stateKind(s: SpawnedState): string {
  return s.context.service.kind;
}

function stateDbPath(s: SpawnedState): string {
  return (s.context.supplier as { location?: string }).location ?? "";
}

function defaultLocationPrefixFromState(s: SpawnedState): string {
  const p = s.context.service.proxyEndpointPrefix || "/";
  const norm = p.replaceAll("\\", "/").replaceAll(/\/+/g, "/").trim();
  return ensureTrailingSlash(norm.startsWith("/") ? norm : `/${norm}`);
}

function upstreamFromState(s: SpawnedState): string {
  return s.context.listen.baseUrl;
}

function nginxHeaderLine(name: string, value: string | number): string {
  // keep header names stable and explicit
  return `    proxy_set_header X-DB-Yard-${name} "${String(value)}";\n`;
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

  const lp = ensureTrailingSlash(locationPrefix).replaceAll(/\/+/g, "/");

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
    proxyPrefix: lp,
  });

  return `# db-yard nginx reverse proxy (generated)
# id=${id}
# db=${dbPath}
# kind=${kind}
# pid=${s.pid}
# upstream=${upstream}
# proxyPrefix=${lp}

# Suggested include filename:
#   db-yard.${name}.${hash}.conf

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

  const routerName = `db-yard-${name}-${hash}`;
  const serviceName = `svc-${name}-${hash}`;
  const mwStripName = `mw-strip-${name}-${hash}`;
  const mwHdrName = `mw-hdr-${name}-${hash}`;

  const mwBlock = `
  middlewares:
    ${mwHdrName}:
      headers:
        customRequestHeaders:
          X-DB-Yard-Id: "${yamlEscape(id)}"
          X-DB-Yard-Db: "${yamlEscape(dbPath)}"
          X-DB-Yard-Kind: "${yamlEscape(kind)}"
          X-DB-Yard-Pid: "${yamlEscape(String(s.pid))}"
          X-DB-Yard-Upstream: "${yamlEscape(url)}"
          X-DB-Yard-ProxyPrefix: "${yamlEscape(lpNoTrail + "/")}"${
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

  return `# db-yard traefik dynamic config (generated)
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
  spawnStateHome: string;
  nginxConfHome?: string;
  traefikConfHome?: string;
  verbose?: boolean;
  includeDead?: boolean; // default false
  overrides?: ProxyConfOverrides;
}) {
  const spawnStateHome = normalizePath(args.spawnStateHome);
  const includeDead = args.includeDead ?? false;
  const overrides = args.overrides ?? {};

  const states: SpawnedState[] = [];
  for await (const s of spawnedStates(spawnStateHome)) {
    if (!includeDead && !s.pidAlive) continue;
    states.push(s);
  }

  if (args.nginxConfHome) {
    const dir = normalizePath(args.nginxConfHome);
    await ensureDir(dir);

    for (const s of states) {
      const id = stateId(s);
      const fn = `db-yard.${safeFileName(id)}.conf`;
      await writeTextAtomic(
        `${dir}/${fn}`,
        nginxReverseProxyConfFromState(s, overrides),
      );
    }

    const bundle = states.map((s) =>
      nginxReverseProxyConfFromState(s, overrides)
    )
      .join("\n");
    await writeTextAtomic(`${dir}/db-yard.generated.conf`, bundle);

    if (args.verbose) {
      console.log(
        `[spawned] wrote nginx conf(s) to: ${dir} (and db-yard.generated.conf)`,
      );
    }
  }

  if (args.traefikConfHome) {
    const dir = normalizePath(args.traefikConfHome);
    await ensureDir(dir);

    for (const s of states) {
      const id = stateId(s);
      const fn = `db-yard.${safeFileName(id)}.yaml`;
      await writeTextAtomic(
        `${dir}/${fn}`,
        traefikReverseProxyConfFromState(s, overrides),
      );
    }

    const bundle = states.map((s) =>
      traefikReverseProxyConfFromState(s, overrides)
    )
      .join("\n");
    await writeTextAtomic(`${dir}/db-yard.generated.yaml`, bundle);

    if (args.verbose) {
      console.log(
        `[spawned] wrote traefik conf(s) to: ${dir} (and db-yard.generated.yaml)`,
      );
    }
  }

  if (!args.nginxConfHome && !args.traefikConfHome) {
    console.log(
      states.map((s) => nginxReverseProxyConfFromState(s, overrides)).join(
        "\n",
      ),
    );
  }
}
