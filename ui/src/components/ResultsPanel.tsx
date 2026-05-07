import {
  Paper, Typography, Alert, Chip, Box,
  Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
  Grid, Accordion, AccordionSummary, AccordionDetails,
  IconButton, Tooltip, Stack,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import DeleteOutlinedIcon from '@mui/icons-material/DeleteOutlined';
import type { ProgressData } from '../api';

interface ResultsPanelProps {
  history: ProgressData[];
  onDelete: (index: number) => void;
}

function formatTimestamp(ms: number): string {
  if (!ms) return 'Unknown';
  const d = new Date(ms);
  return d.toLocaleString(undefined, {
    year: 'numeric', month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  });
}

function RunSummary({ data }: { data: ProgressData }) {
  const hasDiffs = data.totalMissingRecords > 0 || data.recordsDifferent > 0;
  return (
    <Stack direction="row" spacing={1} sx={{ alignItems: 'center', flexWrap: 'wrap', gap: 0.5 }}>
      <Chip
        label={data.state === 'ERROR' ? 'Error' : hasDiffs ? 'Differences Found' : 'Clusters Match'}
        color={data.state === 'ERROR' ? 'warning' : hasDiffs ? 'error' : 'success'}
        size="small"
        sx={{ fontWeight: 600 }}
      />
      <Typography variant="body2" color="text.secondary">
        {data.totalRecordsCompared.toLocaleString()} compared
      </Typography>
      {data.totalMissingRecords > 0 && (
        <Typography variant="body2" color="error.main" sx={{ fontWeight: 600 }}>
          {data.totalMissingRecords.toLocaleString()} missing
        </Typography>
      )}
      {data.recordsDifferent > 0 && (
        <Typography variant="body2" color="error.main" sx={{ fontWeight: 600 }}>
          {data.recordsDifferent.toLocaleString()} different
        </Typography>
      )}
    </Stack>
  );
}

function RunDetail({ data }: { data: ProgressData }) {
  const hasDiffs = data.totalMissingRecords > 0 || data.recordsDifferent > 0;
  return (
    <Box>
      {data.forceTerminated && (
        <Alert severity="warning" sx={{ mb: 2 }}>Comparison was terminated early</Alert>
      )}
      {data.state === 'ERROR' && (
        <Alert severity="error" sx={{ mb: 2 }}>The comparison encountered an error.</Alert>
      )}
      <Box sx={{ mb: 2 }}>
        <Chip
          label={hasDiffs ? 'Differences Found' : 'Clusters Match'}
          color={hasDiffs ? 'error' : 'success'}
          sx={{ fontWeight: 700, fontSize: 14, py: 0.5 }}
        />
      </Box>
      <Grid container spacing={2} sx={{ mb: 3 }}>
        <Grid size={{ xs: 6, sm: 3 }}>
          <Paper variant="outlined" sx={{ p: 2, textAlign: 'center' }}>
            <Typography variant="caption" color="text.secondary">Total Missing</Typography>
            <Typography variant="h6" sx={{ fontWeight: 700 }}>{data.totalMissingRecords.toLocaleString()}</Typography>
          </Paper>
        </Grid>
        <Grid size={{ xs: 6, sm: 3 }}>
          <Paper variant="outlined" sx={{ p: 2, textAlign: 'center' }}>
            <Typography variant="caption" color="text.secondary">Records Different</Typography>
            <Typography variant="h6" sx={{ fontWeight: 700 }}>{data.recordsDifferent.toLocaleString()}</Typography>
          </Paper>
        </Grid>
        <Grid size={{ xs: 6, sm: 3 }}>
          <Paper variant="outlined" sx={{ p: 2, textAlign: 'center' }}>
            <Typography variant="caption" color="text.secondary">Total Compared</Typography>
            <Typography variant="h6" sx={{ fontWeight: 700 }}>{data.totalRecordsCompared.toLocaleString()}</Typography>
          </Paper>
        </Grid>
        <Grid size={{ xs: 6, sm: 3 }}>
          <Paper variant="outlined" sx={{ p: 2, textAlign: 'center' }}>
            <Typography variant="caption" color="text.secondary">Partitions</Typography>
            <Typography variant="h6" sx={{ fontWeight: 700 }}>{data.partitionsComplete}/{data.totalPartitions}</Typography>
          </Paper>
        </Grid>
      </Grid>

      {data.outputFile && (
        <Paper variant="outlined" sx={{ p: 2, mb: 3 }}>
          <Typography variant="caption" color="text.secondary">Output File</Typography>
          <Typography variant="body2" sx={{ fontFamily: 'monospace', wordBreak: 'break-all' }}>{data.outputFile}</Typography>
        </Paper>
      )}

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
            {data.recordsProcessedPerCluster.map((processed, idx) => (
              <TableRow key={idx}>
                <TableCell>Cluster {idx + 1}</TableCell>
                <TableCell align="right">{processed.toLocaleString()}</TableCell>
                <TableCell
                  align="right"
                  sx={{
                    color: (data.recordsMissingPerCluster[idx] || 0) > 0 ? 'error.main' : undefined,
                    fontWeight: (data.recordsMissingPerCluster[idx] || 0) > 0 ? 700 : undefined,
                  }}
                >
                  {(data.recordsMissingPerCluster[idx] || 0).toLocaleString()}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
}

export default function ResultsPanel({ history, onDelete }: ResultsPanelProps) {
  if (history.length === 0) {
    return (
      <Paper sx={{ p: 4, textAlign: 'center', boxShadow: 2 }}>
        <Typography color="text.secondary">No comparison results yet. Results will appear here after a comparison completes.</Typography>
      </Paper>
    );
  }

  return (
    <Box>
      {[...history].reverse().map((run, reversedIdx) => {
        const originalIdx = history.length - 1 - reversedIdx;
        const isLatest = reversedIdx === 0;
        return (
          <Accordion
            key={`${originalIdx}-${run.completedAt}`}
            defaultExpanded={isLatest}
            sx={{ mb: 1, '&:before': { display: 'none' }, boxShadow: 2, borderRadius: '10px !important' }}
          >
            <AccordionSummary expandIcon={<ExpandMoreIcon />} sx={{ borderRadius: '10px' }}>
              <Stack direction="row" spacing={1.5} sx={{ width: '100%', mr: 1, alignItems: 'center', flexWrap: 'wrap' }}>
                <Typography variant="body2" sx={{ fontWeight: 600, minWidth: 170 }}>
                  {formatTimestamp(run.completedAt)}
                </Typography>
                <RunSummary data={run} />
                <Box sx={{ flex: 1 }} />
                <Tooltip title="Delete this result">
                  <IconButton
                    size="small"
                    color="error"
                    onClick={(e) => { e.stopPropagation(); onDelete(originalIdx); }}
                  >
                    <DeleteOutlinedIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              </Stack>
            </AccordionSummary>
            <AccordionDetails>
              <RunDetail data={run} />
            </AccordionDetails>
          </Accordion>
        );
      })}
    </Box>
  );
}
