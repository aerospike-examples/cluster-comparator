import { useEffect, useState, useCallback, useRef } from 'react';
import {
  ThemeProvider, createTheme, CssBaseline,
  AppBar, Toolbar, Typography, Container, Box,
  Tabs, Tab, Alert, Snackbar, Badge,
  Dialog, DialogTitle, DialogContent, DialogContentText, DialogActions, Button,
} from '@mui/material';
import LoginPage from './components/LoginPage';
import ConnectionPanel from './components/ConnectionPanel';
import ComparisonPanel from './components/ComparisonPanel';
import ControlBar from './components/ControlBar';
import ProgressPanel from './components/ProgressPanel';
import ResultsPanel from './components/ResultsPanel';
import {
  checkAuthRequired, login, setAuthToken, getOptions, putOptions,
  startComparison, stopComparison, resetComparison, subscribeProgress,
  fetchClusterMetadata, getResultsHistory, deleteResultHistoryEntry,
  type ProgressData, type ClusterConfig,
} from './api';

const theme = createTheme({
  palette: {
    primary: { main: '#1565c0' },
    secondary: { main: '#e91e63' },
    background: { default: '#f4f6f8' },
  },
  shape: { borderRadius: 10 },
  typography: {
    fontFamily: '"Inter", "Roboto", "Helvetica Neue", Arial, sans-serif',
    h6: { fontWeight: 700 },
  },
  components: {
    MuiTextField: {
      defaultProps: { size: 'small', variant: 'outlined' },
    },
    MuiButton: {
      styleOverrides: {
        root: { textTransform: 'none', fontWeight: 600 },
      },
    },
    MuiPaper: {
      styleOverrides: {
        rounded: { borderRadius: 10 },
      },
    },
    MuiAutocomplete: {
      styleOverrides: {
        listbox: {
          backgroundColor: '#f0f4fa',
          border: '1px solid #bbdefb',
          borderRadius: 8,
          boxShadow: '0 4px 12px rgba(0,0,0,0.12)',
          '& .MuiAutocomplete-option': {
            borderBottom: '1px solid #e3eaf2',
            '&:last-of-type': { borderBottom: 'none' },
            '&[aria-selected="true"]': { backgroundColor: '#d6e4f0' },
            '&.Mui-focused': { backgroundColor: '#e3f2fd' },
          },
        },
        popper: {
          marginTop: '4px !important',
        },
      },
    },
  },
});

function TabPanel({ children, value, index }: { children: React.ReactNode; value: number; index: number }) {
  return value === index ? <Box sx={{ pt: 2 }}>{children}</Box> : null;
}

export default function App() {
  const [authRequired, setAuthRequired] = useState<boolean | null>(null);
  const [authenticated, setAuthenticated] = useState(false);
  const [options, setOptions] = useState<Record<string, unknown>>({});
  const [progress, setProgress] = useState<ProgressData | null>(null);
  const [runState, setRunState] = useState('IDLE');
  const [fieldErrors, setFieldErrors] = useState<Record<string, string> | undefined>();
  const [startError, setStartError] = useState<string | null>(null);
  const [snack, setSnack] = useState<{ msg: string; severity: 'success' | 'error' | 'info' } | null>(null);
  const [tab, setTab] = useState(0);
  const [availableNamespaces, setAvailableNamespaces] = useState<string[]>([]);
  const [availableSets, setAvailableSets] = useState<string[]>([]);
  const [runHistory, setRunHistory] = useState<ProgressData[]>([]);
  const eventSourceRef = useRef<EventSource | null>(null);
  const prevStateRef = useRef<string>('IDLE');
  const outputFileInputRef = useRef<HTMLInputElement | null>(null);
  const [noOutputFileDialogOpen, setNoOutputFileDialogOpen] = useState(false);

  useEffect(() => {
    checkAuthRequired().then((required) => {
      setAuthRequired(required);
      if (!required) setAuthenticated(true);
    });
  }, []);

  useEffect(() => {
    if (authenticated) getOptions().then(setOptions);
  }, [authenticated]);

  useEffect(() => {
    if (!authenticated) return;
    getResultsHistory().then(setRunHistory).catch(() => {});
    const es = subscribeProgress((data) => {
      setProgress(data);
      const newState = data.state || 'IDLE';
      setRunState(newState);
      if ((newState === 'COMPLETE' || newState === 'ERROR') && prevStateRef.current === 'RUNNING') {
        getResultsHistory().then(setRunHistory).catch(() => {});
      }
      prevStateRef.current = newState;
    });
    eventSourceRef.current = es;
    return () => es.close();
  }, [authenticated]);

  const handleLogin = useCallback(async (password: string) => {
    const token = await login(password);
    setAuthToken(token);
    setAuthenticated(true);
  }, []);

  const refreshClusterMetadata = useCallback(async (opts: Record<string, unknown>) => {
    const clusters = opts.clusters as ClusterConfig[] | undefined;
    if (!clusters || clusters.length === 0) return;
    const hasHost = clusters.some(c => c.hostName?.trim());
    if (!hasHost) return;
    try {
      const meta = await fetchClusterMetadata(clusters);
      setAvailableNamespaces(meta.namespaces || []);
      setAvailableSets(meta.sets || []);
    } catch {
      // silently ignore - clusters may not be reachable
    }
  }, []);

  const handleOptionsChange = useCallback((key: string, value: unknown) => {
    setOptions((prev) => {
      const next = { ...prev, [key]: value };
      putOptions(next);
      return next;
    });
    if (fieldErrors?.[key]) {
      setFieldErrors((prev) => {
        if (!prev) return prev;
        const next = { ...prev };
        delete next[key];
        return Object.keys(next).length > 0 ? next : undefined;
      });
    }
    if (startError) setStartError(null);
  }, [fieldErrors, startError]);

  const runStartComparison = useCallback(async () => {
    const result = await startComparison();
    if (result.error) {
      setStartError(result.error);
      if (result.fieldErrors) setFieldErrors(result.fieldErrors);
      setSnack({ msg: result.error, severity: 'error' });
    } else {
      setRunState('RUNNING');
      setSnack({ msg: 'Comparison started', severity: 'success' });
    }
  }, []);

  const handleStart = useCallback(async () => {
    setFieldErrors(undefined);
    setStartError(null);
    const action = String(options.action ?? 'SCAN').toUpperCase();
    const isScanLike = action === 'SCAN' || action.startsWith('SCAN_');
    const hasOutput = String(options.file ?? '').trim().length > 0;
    if (isScanLike && !hasOutput) {
      setNoOutputFileDialogOpen(true);
      return;
    }
    await runStartComparison();
  }, [options.action, options.file, runStartComparison]);

  const handleNoOutputFileContinue = useCallback(async () => {
    setNoOutputFileDialogOpen(false);
    await runStartComparison();
  }, [runStartComparison]);

  const handleNoOutputFileGoBack = useCallback(() => {
    setNoOutputFileDialogOpen(false);
    setTab(1);
    setTimeout(() => {
      outputFileInputRef.current?.focus();
    }, 150);
  }, []);

  const handleStop = useCallback(async () => {
    await stopComparison();
    setSnack({ msg: 'Stop requested', severity: 'info' });
  }, []);

  const handleDeleteHistory = useCallback(async (index: number) => {
    await deleteResultHistoryEntry(index);
    setRunHistory((prev) => prev.filter((_, i) => i !== index));
  }, []);

  const handleReset = useCallback(async () => {
    await resetComparison();
    setRunState('IDLE');
    setProgress(null);
    setFieldErrors(undefined);
    setStartError(null);
    setSnack({ msg: 'Session reset', severity: 'info' });
  }, []);

  const connectionFields = new Set(['clusters', 'hosts1', 'hosts2']);
  const connectionErrorCount = fieldErrors
    ? Object.keys(fieldErrors).filter(k => connectionFields.has(k)).length : 0;
  const comparisonErrorCount = fieldErrors
    ? Object.keys(fieldErrors).filter(k => !connectionFields.has(k)).length : 0;

  function tabLabel(label: string, errorCount: number) {
    if (errorCount === 0) return label;
    return (
      <Badge badgeContent={errorCount} color="error" sx={{ '& .MuiBadge-badge': { fontSize: 11, height: 18, minWidth: 18 } }}>
        {label}
      </Badge>
    );
  }

  if (authRequired === null) return null;

  if (authRequired && !authenticated) {
    return (
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <LoginPage onLogin={handleLogin} />
      </ThemeProvider>
    );
  }

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <AppBar position="static" elevation={2} sx={{ background: 'linear-gradient(135deg, #0d47a1 0%, #1565c0 60%, #1e88e5 100%)' }}>
        <Toolbar sx={{ justifyContent: 'space-between' }}>
          <Typography variant="h6" sx={{ letterSpacing: 0.5 }}>
            Aerospike Cluster Comparator
          </Typography>
          <ControlBar state={runState} onStart={handleStart} onStop={handleStop} onReset={handleReset} />
        </Toolbar>
      </AppBar>
      <Container maxWidth="lg" sx={{ mt: 3, mb: 4 }}>
        {startError && (
          <Alert severity="error" onClose={() => setStartError(null)} sx={{ mb: 2, borderRadius: 2 }}>
            {startError}
          </Alert>
        )}
        <Tabs value={tab} onChange={(_, v) => { setTab(v); if (v === 1) refreshClusterMetadata(options); }} variant="fullWidth"
          sx={{ bgcolor: 'background.paper', borderRadius: 2, mb: 1, boxShadow: 1 }}
        >
          <Tab label={tabLabel("Connections", connectionErrorCount)} />
          <Tab label={tabLabel("Comparison", comparisonErrorCount)} />
          <Tab label="Progress" />
          <Tab label="Results" />
        </Tabs>
        <TabPanel value={tab} index={0}>
          <ConnectionPanel options={options} onChange={handleOptionsChange} fieldErrors={fieldErrors} />
        </TabPanel>
        <TabPanel value={tab} index={1}>
          <ComparisonPanel
            options={options}
            onChange={handleOptionsChange}
            fieldErrors={fieldErrors}
            availableNamespaces={availableNamespaces}
            availableSets={availableSets}
            onRefreshMetadata={() => refreshClusterMetadata(options)}
            outputFileInputRef={outputFileInputRef}
          />
        </TabPanel>
        <TabPanel value={tab} index={2}>
          <ProgressPanel progress={progress} state={runState} />
        </TabPanel>
        <TabPanel value={tab} index={3}>
          <ResultsPanel history={runHistory} onDelete={handleDeleteHistory} />
        </TabPanel>
      </Container>
      <Dialog open={noOutputFileDialogOpen} onClose={handleNoOutputFileGoBack} maxWidth="sm" fullWidth>
        <DialogTitle>No output file</DialogTitle>
        <DialogContent>
          <DialogContentText>
            Scan actions normally write differences to a CSV file. You have not set an output file path.
            You can continue without a file (results will only appear in the console if enabled), or go back
            to the Comparison tab to set the output file.
          </DialogContentText>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={handleNoOutputFileGoBack} variant="contained">
            Set output file
          </Button>
          <Button onClick={() => { void handleNoOutputFileContinue(); }} color="warning">
            Continue anyway
          </Button>
        </DialogActions>
      </Dialog>
      <Snackbar
        open={!!snack}
        autoHideDuration={4000}
        onClose={() => setSnack(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        {snack ? (
          <Alert onClose={() => setSnack(null)} severity={snack.severity} variant="filled" sx={{ width: '100%' }}>
            {snack.msg}
          </Alert>
        ) : undefined}
      </Snackbar>
    </ThemeProvider>
  );
}
