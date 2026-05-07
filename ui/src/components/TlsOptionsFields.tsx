import { useEffect, useState, useCallback, useRef } from 'react';
import {
  TextField, Switch, FormControlLabel, Stack, Box, Typography, Divider, Alert, Button,
  Autocomplete, Link,
} from '@mui/material';
import {
  emptyTlsForm,
  parseTls,
  buildTlsJson,
  type TlsFormState,
} from '../tlsFormUtils';
import { fetchJvmTlsParams } from '../api';

export interface TlsOptionsFieldsProps {
  value: string | undefined;
  onChange: (tls: string) => void;
}

function splitCsv(s: string): string[] {
  return s.split(',').map((x) => x.trim()).filter(Boolean);
}

export default function TlsOptionsFields({ value, onChange }: TlsOptionsFieldsProps) {
  const [form, setForm] = useState<TlsFormState>(emptyTlsForm);
  const [advancedText, setAdvancedText] = useState(value || '');
  const [parseOk, setParseOk] = useState(true);
  const [jvmProtocols, setJvmProtocols] = useState<string[]>([]);
  const [jvmCiphers, setJvmCiphers] = useState<string[]>([]);
  const stashedTlsRef = useRef<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetchJvmTlsParams()
      .then((d) => {
        if (!cancelled) {
          setJvmProtocols(d.protocols ?? []);
          setJvmCiphers(d.ciphers ?? []);
        }
      })
      .catch(() => { /* optional — UI still allows free text */ });
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    const v = value || '';
    setAdvancedText(v);
    const r = parseTls(v);
    if (r.ok) {
      setParseOk(true);
      setForm(r.form);
    } else {
      setParseOk(false);
      setForm({ ...emptyTlsForm(), enabled: true });
    }
  }, [value]);

  const pushForm = useCallback(
    (next: TlsFormState) => {
      setForm(next);
      setParseOk(true);
      const json = buildTlsJson(next);
      setAdvancedText(json);
      onChange(json);
    },
    [onChange],
  );

  const setEnabled = (enabled: boolean) => {
    if (!enabled) {
      if (form.enabled || (!parseOk && (value || '').trim())) {
        const snapshot = parseOk ? buildTlsJson({ ...form, enabled: true }) : advancedText.trim();
        if (snapshot) stashedTlsRef.current = snapshot;
      }
      setForm(emptyTlsForm());
      setParseOk(true);
      setAdvancedText('');
      onChange('');
      return;
    }
    const snap = stashedTlsRef.current;
    if (snap) {
      const r = parseTls(snap);
      if (r.ok) {
        setForm(r.form);
        setParseOk(true);
        setAdvancedText(snap);
        onChange(snap);
        return;
      }
    }
    const fresh: TlsFormState = { ...emptyTlsForm(), enabled: true };
    setForm(fresh);
    setParseOk(true);
    setAdvancedText('{}');
    onChange('{}');
  };

  const updateField = <K extends keyof TlsFormState>(key: K, val: TlsFormState[K]) => {
    const next = { ...form, [key]: val, enabled: true };
    pushForm(next);
  };

  const handleAdvancedChange = (text: string) => {
    setAdvancedText(text);
    onChange(text);
    const r = parseTls(text);
    if (r.ok) {
      setParseOk(true);
      setForm(r.form);
    } else {
      setParseOk(false);
    }
  };

  const handleAdvancedBlur = () => {
    const r = parseTls(advancedText);
    if (r.ok) {
      setParseOk(true);
      setForm(r.form);
      const normalized = buildTlsJson(r.form);
      if (normalized !== advancedText) {
        setAdvancedText(normalized);
        onChange(normalized);
      }
    }
  };

  return (
    <Stack spacing={2} sx={{ width: '100%' }}>
      <FormControlLabel
        control={
          <Switch
            checked={form.enabled || (!parseOk && Boolean((value || '').trim()))}
            onChange={(e) => setEnabled(e.target.checked)}
          />
        }
        label="Use TLS for this cluster"
      />

      {(form.enabled || (!parseOk && (value || '').trim())) && (
        <>
          {!parseOk && (
            <Alert
              severity="warning"
              action={(
                <Button
                  color="inherit"
                  size="small"
                  onClick={() => {
                    setParseOk(true);
                    setForm(emptyTlsForm());
                    setAdvancedText('');
                    onChange('');
                  }}
                >
                  Clear TLS
                </Button>
              )}
            >
              TLS options are not valid JSON. Fix the advanced field or clear and use the form above.
            </Alert>
          )}

          <Typography variant="subtitle2" color="text.secondary">
            Certificate files (PEM paths on this machine)
          </Typography>
          <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: -0.5 }}>
            The comparator loads PEM material with the
            {' '}
            <Link href="https://github.com/Hakky54/sslcontext-kickstart" target="_blank" rel="noopener noreferrer">
              sslcontext-kickstart
            </Link>
            {' '}
            PEM utilities (
            <Link href="https://github.com/Hakky54/sslcontext-kickstart/tree/master/sslcontext-kickstart-for-pem" target="_blank" rel="noopener noreferrer">
              sslcontext-kickstart-for-pem
            </Link>
            ), not the JDK’s JKS keytool format.
          </Typography>
          <Stack spacing={1.5}>
            <TextField
              fullWidth
              label="CA certificate chain"
              value={form.caCertChain}
              onChange={(e) => updateField('caCertChain', e.target.value)}
              placeholder="/path/to/ca.pem"
              disabled={!parseOk}
              helperText="Trust store for verifying the server (common for TLS to Aerospike)"
            />
            <TextField
              fullWidth
              label="Client certificate chain"
              value={form.certChain}
              onChange={(e) => updateField('certChain', e.target.value)}
              placeholder="/path/to/client.pem"
              disabled={!parseOk}
            />
            <TextField
              fullWidth
              label="Client private key"
              value={form.privateKey}
              onChange={(e) => updateField('privateKey', e.target.value)}
              placeholder="/path/to/client.key"
              disabled={!parseOk}
            />
            <TextField
              fullWidth
              label="Key password"
              type="password"
              value={form.keyPassword}
              onChange={(e) => updateField('keyPassword', e.target.value)}
              placeholder="Optional — if the private key is encrypted"
              disabled={!parseOk}
            />
          </Stack>

          <Divider />

          <Typography variant="subtitle2" color="text.secondary">TLS policy</Typography>
          <Autocomplete
            multiple
            freeSolo
            disabled={!parseOk}
            options={jvmProtocols}
            value={splitCsv(form.protocols)}
            onChange={(_, v) => updateField('protocols', Array.isArray(v) ? v.join(',') : String(v))}
            renderInput={(params) => (
              <TextField
                {...params}
                label="Protocols"
                placeholder="Pick from this JVM or type names"
                helperText="Comma-separated TLS protocol names supported by this JVM (and any custom name you add)"
              />
            )}
          />
          <Autocomplete
            multiple
            freeSolo
            disabled={!parseOk}
            options={jvmCiphers}
            value={splitCsv(form.ciphers)}
            onChange={(_, v) => updateField('ciphers', Array.isArray(v) ? v.join(',') : String(v))}
            renderInput={(params) => (
              <TextField
                {...params}
                label="Ciphers"
                placeholder="Optional — pick or type cipher suite names"
                helperText="Cipher suites enabled for this JVM; you can still type suites not listed"
              />
            )}
          />
          <FormControlLabel
            control={(
              <Switch
                checked={form.loginOnly}
                onChange={(e) => updateField('loginOnly', e.target.checked)}
                disabled={!parseOk}
              />
            )}
            label="TLS for login only (forLoginOnly)"
          />
          <TextField
            fullWidth
            label="Revoke certificates (serial numbers)"
            value={form.revokeCerts}
            onChange={(e) => updateField('revokeCerts', e.target.value)}
            placeholder="Optional — format as expected by the comparator"
            disabled={!parseOk}
          />

          <Divider />

          <Box>
            <Typography variant="subtitle2" color="text.secondary" gutterBottom>
              TLS options (JSON)
            </Typography>
            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
              Generated from the fields above. Edit directly for advanced options; on blur, valid JSON is parsed back into the form.
            </Typography>
            <TextField
              fullWidth
              multiline
              minRows={4}
              value={advancedText}
              onChange={(e) => handleAdvancedChange(e.target.value)}
              onBlur={handleAdvancedBlur}
              placeholder='{"context":{"certChain":"client.pem","privateKey":"client.key","caCertChain":"ca.pem"},"protocols":"TLSv1.2"}'
              slotProps={{ input: { sx: { fontFamily: 'monospace', fontSize: 13 } } }}
            />
          </Box>
        </>
      )}
    </Stack>
  );
}
