/**
 * TLS options match com.aerospike.comparator.TlsOptions / SSLOptions
 * (same JSON shape as --tls1 / YAML tls:).
 */

export type TlsFormState = {
  enabled: boolean;
  certChain: string;
  privateKey: string;
  caCertChain: string;
  keyPassword: string;
  protocols: string;
  ciphers: string;
  loginOnly: boolean;
  revokeCerts: string;
};

export type ParseTlsResult =
  | { ok: true; form: TlsFormState }
  | { ok: false; raw: string };

export function emptyTlsForm(): TlsFormState {
  return {
    enabled: false,
    certChain: '',
    privateKey: '',
    caCertChain: '',
    keyPassword: '',
    protocols: '',
    ciphers: '',
    loginOnly: false,
    revokeCerts: '',
  };
}

function protocolsToString(v: unknown): string {
  if (v == null) return '';
  if (Array.isArray(v)) return v.join(',');
  return String(v);
}

export function parseTls(tls: string | undefined): ParseTlsResult {
  if (!tls?.trim()) return { ok: true, form: emptyTlsForm() };
  try {
    const o = JSON.parse(tls) as Record<string, unknown>;
    const ctx = o.context && typeof o.context === 'object' && !Array.isArray(o.context)
      ? (o.context as Record<string, unknown>)
      : {};
    const form: TlsFormState = {
      enabled: true,
      certChain: String(ctx.certChain ?? ''),
      privateKey: String(ctx.privateKey ?? ''),
      caCertChain: String(ctx.caCertChain ?? ''),
      keyPassword: String(ctx.keyPassword ?? ''),
      protocols: protocolsToString(o.protocols),
      ciphers: protocolsToString(o.ciphers),
      loginOnly: Boolean(o.loginOnly),
      revokeCerts: String(o.revokeCerts ?? ''),
    };
    return { ok: true, form };
  } catch {
    return { ok: false, raw: tls };
  }
}

export function buildTlsJson(form: TlsFormState): string {
  if (!form.enabled) return '';

  const out: Record<string, unknown> = {};

  const ctx: Record<string, string> = {};
  if (form.certChain.trim()) ctx.certChain = form.certChain.trim();
  if (form.privateKey.trim()) ctx.privateKey = form.privateKey.trim();
  if (form.caCertChain.trim()) ctx.caCertChain = form.caCertChain.trim();
  if (form.keyPassword) ctx.keyPassword = form.keyPassword;

  if (Object.keys(ctx).length > 0) {
    out.context = ctx;
  }

  if (form.protocols.trim()) out.protocols = form.protocols.trim();
  if (form.ciphers.trim()) out.ciphers = form.ciphers.trim();
  if (form.loginOnly) out.loginOnly = true;
  if (form.revokeCerts.trim()) out.revokeCerts = form.revokeCerts.trim();

  if (Object.keys(out).length === 0) {
    return form.enabled ? '{}' : '';
  }

  return JSON.stringify(out);
}
