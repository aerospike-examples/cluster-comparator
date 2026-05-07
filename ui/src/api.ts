let authToken: string | null = null;

export function setAuthToken(token: string) {
  authToken = token;
}

export function getAuthToken(): string | null {
  return authToken;
}

function headers(): Record<string, string> {
  const h: Record<string, string> = { 'Content-Type': 'application/json' };
  if (authToken) {
    h['Authorization'] = `Bearer ${authToken}`;
  }
  return h;
}

export async function checkAuthRequired(): Promise<boolean> {
  const res = await fetch('/api/auth-required');
  const data = await res.json();
  return data.required;
}

export async function login(password: string): Promise<string> {
  const res = await fetch('/api/auth', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ password }),
  });
  if (!res.ok) {
    const data = await res.json();
    throw new Error(data.error || 'Login failed');
  }
  const data = await res.json();
  return data.token;
}

export async function getOptions(): Promise<Record<string, unknown>> {
  const res = await fetch('/api/options', { headers: headers() });
  return res.json();
}

export async function putOptions(opts: Record<string, unknown>): Promise<void> {
  await fetch('/api/options', {
    method: 'PUT',
    headers: headers(),
    body: JSON.stringify(opts),
  });
}

export interface StartResult {
  status?: string;
  error?: string;
  fieldErrors?: Record<string, string>;
}

export async function startComparison(): Promise<StartResult> {
  const res = await fetch('/api/start', {
    method: 'POST',
    headers: headers(),
  });
  return res.json();
}

export async function stopComparison(): Promise<void> {
  await fetch('/api/stop', { method: 'POST', headers: headers() });
}

export async function resetComparison(): Promise<void> {
  await fetch('/api/reset', { method: 'POST', headers: headers() });
}

export async function getStatus(): Promise<Record<string, unknown>> {
  const res = await fetch('/api/status', { headers: headers() });
  return res.json();
}

export interface JvmTlsParams {
  protocols: string[];
  ciphers: string[];
}

export async function fetchJvmTlsParams(): Promise<JvmTlsParams> {
  const res = await fetch('/api/jvm-tls-params', { headers: headers() });
  if (!res.ok) {
    const data = (await res.json().catch(() => ({}))) as { error?: string };
    throw new Error(data.error || res.statusText);
  }
  return res.json();
}

export interface ClusterConfig {
  hostName?: string;
  clusterName?: string;
  userName?: string;
  password?: string;
  authMode?: string;
  tls?: string;
  useServicesAlternate?: boolean;
}

export interface TestConnectionResult {
  success: boolean;
  nodes?: number;
  namespaces?: string[];
  versions?: string[];
  clusterName?: string;
  error?: string;
}

export async function testConnection(cluster: ClusterConfig): Promise<TestConnectionResult> {
  const res = await fetch('/api/test-connection', {
    method: 'POST',
    headers: headers(),
    body: JSON.stringify(cluster),
  });
  return res.json();
}

export interface PopulateDataResult {
  success: boolean;
  recordsWritten?: number;
  error?: string;
  cancelled?: boolean;
  firstKey?: string;
  lastKey?: string;
}

export async function populateData(
  cluster: ClusterConfig,
  namespace: string,
  set: string,
  count: number,
  opts?: { startKey?: number; generationMode?: 'random' | 'constant' },
): Promise<PopulateDataResult> {
  const body: Record<string, unknown> = {
    cluster,
    namespace,
    set,
    count,
    startKey: opts?.startKey ?? 1,
    generationMode: opts?.generationMode ?? 'random',
  };
  const res = await fetch('/api/populate-data', {
    method: 'POST',
    headers: headers(),
    body: JSON.stringify(body),
  });
  return (await res.json()) as PopulateDataResult;
}

export async function cancelPopulateData(): Promise<void> {
  await fetch('/api/populate-cancel', {
    method: 'POST',
    headers: headers(),
  });
}

export interface PopulateProgress {
  current: number;
  total: number;
}

export async function getPopulateProgress(): Promise<PopulateProgress> {
  const res = await fetch('/api/populate-progress', { headers: headers() });
  return res.json();
}

export interface ClusterMetadataResult {
  namespaces: string[];
  sets: string[];
}

export async function fetchClusterMetadata(clusters: ClusterConfig[]): Promise<ClusterMetadataResult> {
  const res = await fetch('/api/cluster-metadata', {
    method: 'POST',
    headers: headers(),
    body: JSON.stringify({ clusters }),
  });
  return res.json();
}

export async function getResultsHistory(): Promise<ProgressData[]> {
  const res = await fetch('/api/results-history', { headers: headers() });
  return res.json();
}

export async function deleteResultHistoryEntry(index: number): Promise<void> {
  await fetch(`/api/results-history/${index}`, { method: 'DELETE', headers: headers() });
}

export interface ProgressData {
  recordsProcessedPerCluster: number[];
  recordsMissingPerCluster: number[];
  recordsDifferent: number;
  totalMissingRecords: number;
  totalRecordsCompared: number;
  partitionsComplete: number;
  totalPartitions: number;
  forceTerminated: boolean;
  outputFile: string | null;
  state: string;
  completedAt: number;
}

export function subscribeProgress(
  onData: (data: ProgressData) => void,
  onError?: (e: Event) => void,
): EventSource {
  const url = authToken ? `/api/progress?token=${authToken}` : '/api/progress';
  const es = new EventSource(url);
  es.addEventListener('progress', (evt) => {
    try {
      const data = JSON.parse((evt as MessageEvent).data);
      onData(data);
    } catch {
      // ignore parse errors
    }
  });
  if (onError) {
    es.onerror = onError;
  }
  return es;
}
