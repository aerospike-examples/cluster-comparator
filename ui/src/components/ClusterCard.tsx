import { useState, useEffect, useRef } from 'react';
import {
  Accordion, AccordionSummary, AccordionDetails,
  TextField, Select, MenuItem, FormControl, InputLabel,
  FormControlLabel, Switch, Button, Stack, Box,
  Typography, Chip, Alert, Tooltip, IconButton,
  Dialog, DialogTitle, DialogContent, DialogActions,
  Grid, LinearProgress,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import DeleteOutlinedIcon from '@mui/icons-material/DeleteOutlined';
import CableIcon from '@mui/icons-material/Cable';
import StorageIcon from '@mui/icons-material/Storage';
import SettingsIcon from '@mui/icons-material/Settings';
import FiberManualRecordIcon from '@mui/icons-material/FiberManualRecord';
import { testConnection, populateData, cancelPopulateData, getPopulateProgress, type ClusterConfig, type TestConnectionResult } from '../api';
import TlsOptionsFields from './TlsOptionsFields';

interface ClusterCardProps {
  index: number;
  cluster: ClusterConfig;
  onChange: (cluster: ClusterConfig) => void;
  onRemove: () => void;
  canRemove: boolean;
  hostError?: string;
}

type ConnStatus = 'untested' | 'success' | 'error';

function HelpTip({ text }: { text: string }) {
  return (
    <Tooltip title={text} arrow placement="top">
      <InfoOutlinedIcon sx={{ fontSize: 16, color: 'action.active', ml: 0.5, verticalAlign: 'middle', cursor: 'help' }} />
    </Tooltip>
  );
}

export default function ClusterCard({ index, cluster, onChange, onRemove, canRemove, hostError }: ClusterCardProps) {
  const [connStatus, setConnStatus] = useState<ConnStatus>('untested');
  const [connResult, setConnResult] = useState<TestConnectionResult | null>(null);
  const [testing, setTesting] = useState(false);
  const [popOpen, setPopOpen] = useState(false);
  const [popNs, setPopNs] = useState('test');
  const [popSet, setPopSet] = useState('customers');
  const [popCount, setPopCount] = useState(100);
  const [popStartKey, setPopStartKey] = useState(1);
  const [popGenMode, setPopGenMode] = useState<'random' | 'constant'>('random');
  const [popGenerateSucceeded, setPopGenerateSucceeded] = useState(false);
  const [popLoading, setPopLoading] = useState(false);
  const [popResult, setPopResult] = useState<string | null>(null);
  const [popProgress, setPopProgress] = useState<{ current: number; total: number } | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, []);

  const update = (field: keyof ClusterConfig, value: unknown) => {
    onChange({ ...cluster, [field]: value });
    if (connStatus !== 'untested') setConnStatus('untested');
  };

  const handleTest = async () => {
    setTesting(true);
    setConnResult(null);
    try {
      const result = await testConnection(cluster);
      setConnResult(result);
      setConnStatus(result.success ? 'success' : 'error');
      if (result.success && result.clusterName && !cluster.clusterName) {
        onChange({ ...cluster, clusterName: result.clusterName });
      }
    } catch (e) {
      setConnResult({ success: false, error: e instanceof Error ? e.message : 'Connection failed' });
      setConnStatus('error');
    } finally {
      setTesting(false);
    }
  };

  const handlePopulate = async () => {
    setPopLoading(true);
    setPopResult(null);
    setPopProgress(null);
    pollRef.current = setInterval(async () => {
      try {
        const p = await getPopulateProgress();
        if (p.total > 0) setPopProgress(p);
      } catch { /* ignore */ }
    }, 300);
    try {
      const result = await populateData(cluster, popNs, popSet, popCount, {
        startKey: popStartKey,
        generationMode: popGenMode,
      });
      if (result.cancelled) {
        setPopGenerateSucceeded(false);
        const keyPart = result.firstKey && result.lastKey ? ` (${result.firstKey} … ${result.lastKey})` : '';
        if (result.recordsWritten && result.recordsWritten > 0) {
          setPopResult(`Cancelled after writing ${result.recordsWritten} record(s) to ${popNs}/${popSet}${keyPart}`);
        } else {
          setPopResult('Cancelled before any records were written.');
        }
      } else if (result.success && result.recordsWritten != null) {
        const keyPart = result.firstKey && result.lastKey ? ` (${result.firstKey} … ${result.lastKey})` : '';
        setPopResult(`Wrote ${result.recordsWritten} records to ${popNs}/${popSet}${keyPart}`);
        setPopGenerateSucceeded(true);
      } else {
        setPopGenerateSucceeded(false);
        setPopResult(`Error: ${result.error ?? 'Unknown error'}`);
      }
    } catch (e) {
      setPopResult(`Error: ${e instanceof Error ? e.message : 'Failed'}`);
    } finally {
      if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
      setPopLoading(false);
      setPopProgress(null);
    }
  };

  const isRemote = (cluster.hostName || '').startsWith('remote:');
  const displayName = cluster.clusterName || `Cluster ${index + 1}`;
  const hostSummary = cluster.hostName || '(no host configured)';
  const dotColor = connStatus === 'success' ? 'success' : connStatus === 'error' ? 'error' : 'disabled';

  return (
    <>
      <Accordion defaultExpanded={index < 2} sx={{ mb: 1.5, '&:before': { display: 'none' }, boxShadow: 2, borderRadius: '10px !important' }}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />} sx={{ borderRadius: '10px' }}>
          <Stack direction="row" spacing={1} sx={{ width: '100%', mr: 1, alignItems: 'center' }}>
            <FiberManualRecordIcon color={dotColor} sx={{ fontSize: 14 }} />
            <Typography sx={{ fontWeight: 600 }}>{displayName}</Typography>
            {isRemote && <Chip label="Remote Proxy" size="small" color="info" sx={{ fontSize: 11 }} />}
            <Typography variant="body2" color="text.secondary">{hostSummary}</Typography>
            <Box sx={{ flex: 1 }} />
            {canRemove && (
              <Tooltip title="Remove cluster">
                <IconButton size="small" color="error" onClick={(e) => { e.stopPropagation(); onRemove(); }}>
                  <DeleteOutlinedIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            )}
          </Stack>
        </AccordionSummary>
        <AccordionDetails>
          <Grid container spacing={2}>
            <Grid size={{ xs: 12, sm: 6 }}>
              <Tooltip title="Direct: hostname[:port],... (default port 3000). Remote proxy: remote:<host>:<port>" arrow placement="top">
                <TextField
                  fullWidth label="Hosts" required
                  value={cluster.hostName || ''}
                  onChange={(e) => update('hostName', e.target.value)}
                  placeholder="e.g. 192.168.1.10:3000 or remote:10.0.0.5:3100"
                  error={!!hostError}
                  helperText={hostError || (isRemote ? 'Connecting via remote comparator proxy' : undefined)}
                />
              </Tooltip>
            </Grid>
            <Grid size={{ xs: 12, sm: 6 }}>
              <Tooltip title="Optional cluster name for identification and cluster-name validation." arrow placement="top">
                <TextField
                  fullWidth label="Cluster Name"
                  value={cluster.clusterName || ''}
                  onChange={(e) => update('clusterName', e.target.value)}
                  placeholder="e.g. dc1"
                />
              </Tooltip>
            </Grid>
          </Grid>

          {isRemote && (
            <Alert severity="info" sx={{ mt: 2 }}>
              This cluster uses a remote comparator proxy. Authentication and TLS are configured on the proxy server.
            </Alert>
          )}

          {!isRemote && <Accordion variant="outlined" sx={{ mt: 2, '&:before': { display: 'none' } }}>
            <AccordionSummary expandIcon={<ExpandMoreIcon />}>
              <SettingsIcon sx={{ mr: 1, color: 'text.secondary' }} />
              <Typography variant="body2" color="text.secondary">Advanced Settings</Typography>
            </AccordionSummary>
            <AccordionDetails>
              <Grid container spacing={2}>
                <Grid size={{ xs: 12, sm: 4 }}>
                  <Tooltip title="Database user for authentication." arrow>
                    <TextField fullWidth label="User" value={cluster.userName || ''} onChange={(e) => update('userName', e.target.value)} placeholder="admin" />
                  </Tooltip>
                </Grid>
                <Grid size={{ xs: 12, sm: 4 }}>
                  <Tooltip title="Database password for the specified user." arrow>
                    <TextField fullWidth label="Password" type="password" value={cluster.password || ''} onChange={(e) => update('password', e.target.value)} />
                  </Tooltip>
                </Grid>
                <Grid size={{ xs: 12, sm: 4 }}>
                  <Tooltip title="Authentication mode: INTERNAL (default), EXTERNAL, EXTERNAL_INSECURE, or PKI." arrow>
                    <FormControl fullWidth size="small">
                      <InputLabel>Auth Mode</InputLabel>
                      <Select label="Auth Mode" value={cluster.authMode || 'INTERNAL'} onChange={(e) => update('authMode', e.target.value)}>
                        <MenuItem value="INTERNAL">Internal</MenuItem>
                        <MenuItem value="EXTERNAL">External</MenuItem>
                        <MenuItem value="EXTERNAL_INSECURE">External Insecure</MenuItem>
                        <MenuItem value="PKI">PKI</MenuItem>
                      </Select>
                    </FormControl>
                  </Tooltip>
                </Grid>
                <Grid size={{ xs: 12, sm: 4 }}>
                  <Tooltip title="Use services-alternate addresses when clients connect through NAT or a load balancer." arrow>
                    <FormControlLabel
                      control={<Switch checked={!!cluster.useServicesAlternate} onChange={(e) => update('useServicesAlternate', e.target.checked)} />}
                      label="Services Alternate"
                    />
                  </Tooltip>
                </Grid>
                <Grid size={{ xs: 12 }}>
                  <TlsOptionsFields
                    value={cluster.tls}
                    onChange={(tls) => update('tls', tls)}
                  />
                </Grid>
              </Grid>
            </AccordionDetails>
          </Accordion>}

          <Stack direction="row" spacing={1} sx={{ mt: 2 }}>
            <Button
              variant="outlined"
              startIcon={<CableIcon />}
              onClick={handleTest}
              disabled={!cluster.hostName || testing}
            >
              {testing ? 'Testing…' : 'Test Connection'}
            </Button>
            <Button
              variant="outlined"
              startIcon={<StorageIcon />}
              onClick={() => {
                setPopGenerateSucceeded(false);
                setPopResult(null);
                setPopOpen(true);
              }}
              disabled={!cluster.hostName}
            >
              Populate Test Data
            </Button>
          </Stack>

          {connResult && (
            <Box sx={{ mt: 1.5 }}>
              {connResult.success ? (
                <Alert severity="success">
                  <strong>Connected</strong>
                  {connResult.clusterName && <> — Cluster: <Chip label={connResult.clusterName} size="small" color="primary" sx={{ mx: 0.5 }} /></>}
                  {' '}— Nodes: <Chip label={connResult.nodes} size="small" sx={{ mx: 0.5 }} />
                  Namespaces: {connResult.namespaces?.map(ns => <Chip key={ns} label={ns} size="small" sx={{ mx: 0.25 }} />)}
                  {' '}Version: {connResult.versions?.map(v => <Chip key={v} label={v} size="small" color="success" sx={{ mx: 0.25 }} />)}
                </Alert>
              ) : (
                <Alert severity="error">{connResult.error}</Alert>
              )}
            </Box>
          )}
        </AccordionDetails>
      </Accordion>

      <Dialog open={popOpen} onClose={() => { setPopOpen(false); setPopResult(null); setPopGenerateSucceeded(false); }} maxWidth="xs" fullWidth>
        <DialogTitle>Populate Test Data</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <TextField
              fullWidth
              label={<>Namespace <HelpTip text="Target namespace for test records." /></>}
              value={popNs}
              onChange={(e) => setPopNs(e.target.value)}
            />
            <TextField
              fullWidth
              label={<>Set <HelpTip text="Target set name for test records." /></>}
              value={popSet}
              onChange={(e) => setPopSet(e.target.value)}
            />
            <TextField
              fullWidth
              label={<>Record Count <HelpTip text="Number of customer records (1–100000). User keys are the string customer- plus the decimal id for each id from Start Key through Start Key + Count − 1 (no leading zeros)." /></>}
              type="number"
              value={popCount}
              onChange={(e) => setPopCount(Math.max(1, Math.min(100000, Number(e.target.value) || 1)))}
              slotProps={{ htmlInput: { min: 1, max: 100000 } }}
            />
            <TextField
              fullWidth
              label={<>Start Key <HelpTip text="First numeric id in the user key. With Count 10 and Start Key 50, keys are customer-50 through customer-59 (same string form regardless of other batches)." /></>}
              type="number"
              value={popStartKey}
              onChange={(e) => setPopStartKey(Math.max(0, Math.floor(Number(e.target.value)) || 0))}
              slotProps={{ htmlInput: { min: 0 } }}
            />
            <FormControl fullWidth size="small">
              <InputLabel>Generation</InputLabel>
              <Select
                label="Generation"
                value={popGenMode}
                onChange={(e) => setPopGenMode(e.target.value as 'random' | 'constant')}
                endAdornment={(
                  <HelpTip text="Constant: same bins for the same key on every run (use the same settings on each cluster for identical records). Random: varied bins each run." />
                )}
              >
                <MenuItem value="random">Random (differences)</MenuItem>
                <MenuItem value="constant">Constant (identical)</MenuItem>
              </Select>
            </FormControl>
            {popLoading && popProgress && popProgress.total > 0 && (
              <Box>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                  <Typography variant="body2" color="text.secondary">Writing records…</Typography>
                  <Typography variant="body2" sx={{ fontWeight: 600 }}>{popProgress.current.toLocaleString()} / {popProgress.total.toLocaleString()}</Typography>
                </Box>
                <LinearProgress variant="determinate" value={Math.round((popProgress.current / popProgress.total) * 100)} sx={{ height: 8, borderRadius: 4 }} />
              </Box>
            )}
            {popGenerateSucceeded && (
              <Typography variant="caption" color="text.secondary">
                Close this dialog to run another generation.
              </Typography>
            )}
            {popResult && (
              <Alert
                severity={
                  popResult.startsWith('Error') ? 'error'
                    : popResult.startsWith('Cancelled') ? 'warning'
                      : 'success'
                }
              >
                {popResult}
              </Alert>
            )}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => { setPopOpen(false); setPopResult(null); setPopGenerateSucceeded(false); }}>Close</Button>
          {popLoading && (
            <Button variant="outlined" color="warning" onClick={() => { cancelPopulateData().catch(() => {}); }}>
              Cancel generation
            </Button>
          )}
          {!popLoading && !popGenerateSucceeded && (
            <Button variant="contained" onClick={handlePopulate}>
              Generate
            </Button>
          )}
        </DialogActions>
      </Dialog>
    </>
  );
}
