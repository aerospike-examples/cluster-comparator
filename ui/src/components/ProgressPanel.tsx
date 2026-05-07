import { useRef, useEffect, useState } from 'react';
import {
  Paper, Typography, Box, LinearProgress,
  Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
  Grid, Stack,
} from '@mui/material';
import type { ProgressData } from '../api';

interface ProgressPanelProps {
  progress: ProgressData | null;
  state: string;
}

function StatCard({ label, value }: { label: string; value: string | number }) {
  return (
    <Paper variant="outlined" sx={{ p: 2, textAlign: 'center' }}>
      <Typography variant="caption" color="text.secondary">{label}</Typography>
      <Typography variant="h6" sx={{ fontWeight: 700 }}>{value}</Typography>
    </Paper>
  );
}

export default function ProgressPanel({ progress, state }: ProgressPanelProps) {
  const startTimeRef = useRef<number | null>(null);
  const prevTotalRef = useRef<number>(0);
  const [throughput, setThroughput] = useState(0);

  const endTimeRef = useRef<number | null>(null);

  useEffect(() => {
    if (state === 'RUNNING' && !startTimeRef.current) {
      startTimeRef.current = Date.now();
      endTimeRef.current = null;
    }
    if ((state === 'COMPLETE' || state === 'ERROR') && !endTimeRef.current) {
      endTimeRef.current = Date.now();
    }
    if (state === 'IDLE') {
      startTimeRef.current = null;
      endTimeRef.current = null;
      prevTotalRef.current = 0;
      setThroughput(0);
    }
  }, [state]);

  useEffect(() => {
    if (!progress) return;
    const total = progress.recordsProcessedPerCluster?.reduce((a, b) => a + b, 0) || 0;
    const diff = total - prevTotalRef.current;
    prevTotalRef.current = total;
    if (diff > 0 && progress.recordsProcessedPerCluster.length > 0) {
      setThroughput(Math.round(diff / progress.recordsProcessedPerCluster.length));
    }
  }, [progress]);

  if (!progress || state === 'IDLE') {
    return (
      <Paper sx={{ p: 4, textAlign: 'center', boxShadow: 2 }}>
        <Typography color="text.secondary">No comparison running. Configure options and click Start.</Typography>
      </Paper>
    );
  }

  const pct = progress.totalPartitions > 0 ? Math.round((progress.partitionsComplete / progress.totalPartitions) * 100) : 0;
  const endTime = endTimeRef.current || Date.now();
  const elapsed = startTimeRef.current ? Math.round((endTime - startTimeRef.current) / 1000) : 0;
  const elapsedStr = `${Math.floor(elapsed / 60)}m ${elapsed % 60}s`;

  return (
    <Paper sx={{ p: 3, boxShadow: 2 }}>
      <Grid container spacing={2} sx={{ mb: 3 }}>
        <Grid size={{ xs: 6, sm: 3 }}><StatCard label="Elapsed" value={elapsedStr} /></Grid>
        <Grid size={{ xs: 6, sm: 3 }}><StatCard label="Throughput (rec/s)" value={throughput.toLocaleString()} /></Grid>
        <Grid size={{ xs: 6, sm: 3 }}><StatCard label="Total Missing" value={progress.totalMissingRecords.toLocaleString()} /></Grid>
        <Grid size={{ xs: 6, sm: 3 }}><StatCard label="Records Different" value={progress.recordsDifferent.toLocaleString()} /></Grid>
      </Grid>

      <Stack spacing={0.5} sx={{ mb: 3 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
          <Typography variant="body2">Partitions: {progress.partitionsComplete} / {progress.totalPartitions}</Typography>
          <Typography variant="body2" sx={{ fontWeight: 600 }}>{pct}%</Typography>
        </Box>
        <LinearProgress variant="determinate" value={pct} sx={{ height: 8, borderRadius: 4 }} />
      </Stack>

      <TableContainer>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell><strong>Cluster</strong></TableCell>
              <TableCell align="right"><strong>Records Processed</strong></TableCell>
              <TableCell align="right"><strong>Records Missing</strong></TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {progress.recordsProcessedPerCluster.map((processed, idx) => (
              <TableRow key={idx}>
                <TableCell>Cluster {idx + 1}</TableCell>
                <TableCell align="right">{processed.toLocaleString()}</TableCell>
                <TableCell align="right" sx={{ color: (progress.recordsMissingPerCluster[idx] || 0) > 0 ? 'error.main' : undefined, fontWeight: (progress.recordsMissingPerCluster[idx] || 0) > 0 ? 700 : undefined }}>
                  {(progress.recordsMissingPerCluster[idx] || 0).toLocaleString()}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Paper>
  );
}
