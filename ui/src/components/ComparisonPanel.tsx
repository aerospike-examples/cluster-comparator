import { useState } from 'react';
import type { RefObject } from 'react';
import {
  Paper, TextField, Select, MenuItem, FormControl, InputLabel,
  FormControlLabel, Switch, Tooltip, Typography, Divider,
  Grid, Box, FormHelperText, ToggleButtonGroup, ToggleButton,
  Autocomplete, IconButton, Button, InputAdornment,
} from '@mui/material';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import CalendarMonthIcon from '@mui/icons-material/CalendarMonth';
import NumbersIcon from '@mui/icons-material/Numbers';
import RefreshIcon from '@mui/icons-material/Refresh';

interface ComparisonPanelProps {
  options: Record<string, unknown>;
  onChange: (key: string, value: unknown) => void;
  fieldErrors?: Record<string, string>;
  availableNamespaces?: string[];
  availableSets?: string[];
  onRefreshMetadata?: () => void;
  /** When set, the output file field forwards this ref for programmatic focus (e.g. from App). */
  outputFileInputRef?: RefObject<HTMLInputElement | null>;
}

function err(fieldErrors: Record<string, string> | undefined, key: string): { error?: boolean; helperText?: string } {
  if (!fieldErrors || !fieldErrors[key]) return {};
  return { error: true, helperText: fieldErrors[key] };
}

function HelpTip({ text }: { text: string }) {
  return (
    <Tooltip title={text} arrow placement="top">
      <InfoOutlinedIcon sx={{ fontSize: 16, color: 'action.active', ml: 0.5, verticalAlign: 'middle', cursor: 'help' }} />
    </Tooltip>
  );
}

function msToDateAndTime(ms: string | undefined): { date: string; time: string } {
  if (!ms) return { date: '', time: '' };
  const n = Number(ms);
  if (isNaN(n) || n <= 0) return { date: '', time: '' };
  const d = new Date(n);
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const hh = String(d.getHours()).padStart(2, '0');
  const min = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  return { date: `${yyyy}-${mm}-${dd}`, time: `${hh}:${min}:${ss}` };
}

function dateAndTimeToMs(date: string, time: string): string {
  if (!date) return '';
  const t = time || '00:00:00';
  const parts = t.split(':');
  const hh = parseInt(parts[0] || '0');
  const mm = parseInt(parts[1] || '0');
  const ss = parseInt(parts[2] || '0');
  const d = new Date(date);
  d.setHours(hh, mm, ss, 0);
  return String(d.getTime());
}

function DateTimeInput({ label, helpText, msValue, onChangeMs, fieldError }: {
  label: string;
  helpText: string;
  msValue: string | undefined;
  onChangeMs: (ms: string) => void;
  fieldError?: string;
}) {
  const { date, time } = msToDateAndTime(msValue);
  return (
    <Grid container spacing={1}>
      <Grid size={{ xs: 7 }}>
        <TextField
          fullWidth
          label={<>{label} <HelpTip text={helpText} /></>}
          type="date"
          value={date}
          onChange={(e) => onChangeMs(dateAndTimeToMs(e.target.value, time))}
          slotProps={{ inputLabel: { shrink: true } }}
          error={!!fieldError}
          helperText={fieldError}
        />
      </Grid>
      <Grid size={{ xs: 5 }}>
        <TextField
          fullWidth
          label="Time (24h)"
          value={time}
          onChange={(e) => {
            if (date) onChangeMs(dateAndTimeToMs(date, e.target.value));
          }}
          placeholder="HH:mm:ss"
          slotProps={{ inputLabel: { shrink: true } }}
          disabled={!date}
        />
      </Grid>
    </Grid>
  );
}

function DateRangeSection({ options, onChange, fieldErrors }: {
  options: Record<string, unknown>;
  onChange: (key: string, value: unknown) => void;
  fieldErrors?: Record<string, string>;
}) {
  const [mode, setMode] = useState<'picker' | 'millis'>('picker');

  return (
    <>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 1.5 }}>
        <Typography variant="subtitle1" sx={{ fontWeight: 700 }}>Date Range Filter</Typography>
        <ToggleButtonGroup
          value={mode}
          exclusive
          onChange={(_, v) => { if (v) setMode(v); }}
          size="small"
        >
          <ToggleButton value="picker">
            <CalendarMonthIcon sx={{ fontSize: 18, mr: 0.5 }} /> Date/Time
          </ToggleButton>
          <ToggleButton value="millis">
            <NumbersIcon sx={{ fontSize: 18, mr: 0.5 }} /> Milliseconds
          </ToggleButton>
        </ToggleButtonGroup>
      </Box>
      <Grid container spacing={2}>
        {mode === 'picker' ? (
          <>
            <Grid size={{ xs: 12, sm: 6 }}>
              <DateTimeInput
                label="Begin Date"
                helpText="Only include records updated at or after this date and time."
                msValue={options.beginDate as string}
                onChangeMs={(ms) => onChange('beginDate', ms)}
                fieldError={fieldErrors?.beginDate}
              />
            </Grid>
            <Grid size={{ xs: 12, sm: 6 }}>
              <DateTimeInput
                label="End Date"
                helpText="Only include records updated before this date and time."
                msValue={options.endDate as string}
                onChangeMs={(ms) => onChange('endDate', ms)}
                fieldError={fieldErrors?.endDate}
              />
            </Grid>
          </>
        ) : (
          <>
            <Grid size={{ xs: 12, sm: 6 }}>
              <TextField
                fullWidth
                label={<>Begin Date (ms) <HelpTip text="Only include records updated at or after this time, in milliseconds since epoch." /></>}
                type="number"
                value={(options.beginDate as string) || ''}
                onChange={(e) => onChange('beginDate', e.target.value)}
                placeholder="e.g. 1714500000000"
                slotProps={{
                  htmlInput: { min: 0 },
                  input: {
                    endAdornment: (
                      <InputAdornment position="end">
                        <Button size="small" onClick={() => onChange('beginDate', String(Date.now()))}>
                          Now
                        </Button>
                      </InputAdornment>
                    ),
                  },
                }}
                {...err(fieldErrors, 'beginDate')}
              />
            </Grid>
            <Grid size={{ xs: 12, sm: 6 }}>
              <TextField
                fullWidth
                label={<>End Date (ms) <HelpTip text="Only include records updated before this time, in milliseconds since epoch." /></>}
                type="number"
                value={(options.endDate as string) || ''}
                onChange={(e) => onChange('endDate', e.target.value)}
                placeholder="e.g. 1714600000000"
                slotProps={{
                  htmlInput: { min: 0 },
                  input: {
                    endAdornment: (
                      <InputAdornment position="end">
                        <Button size="small" onClick={() => onChange('endDate', String(Date.now()))}>
                          Now
                        </Button>
                      </InputAdornment>
                    ),
                  },
                }}
                {...err(fieldErrors, 'endDate')}
              />
            </Grid>
          </>
        )}
      </Grid>
    </>
  );
}

export default function ComparisonPanel({ options, onChange, fieldErrors, availableNamespaces = [], availableSets = [], onRefreshMetadata, outputFileInputRef }: ComparisonPanelProps) {
  const nsValue = (options.namespaces as string || '').split(',').filter(Boolean);
  const setValue = (options.setNames as string || '').split(',').filter(Boolean);

  return (
    <Paper sx={{ p: 3, boxShadow: 2 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
        <Typography variant="subtitle1" sx={{ fontWeight: 700 }}>Core Settings</Typography>
        <Tooltip title="Refresh namespaces and sets from clusters" arrow placement="top">
          <IconButton size="small" onClick={onRefreshMetadata} color="primary">
            <RefreshIcon fontSize="small" />
          </IconButton>
        </Tooltip>
      </Box>
      <Grid container spacing={2}>
        <Grid size={{ xs: 12, sm: 4 }}>
          <Autocomplete
            multiple
            freeSolo
            options={availableNamespaces}
            value={nsValue}
            onChange={(_, newVal) => onChange('namespaces', newVal.join(','))}
            renderInput={(params) => (
              <TextField
                {...params}
                label={<>Namespaces <HelpTip text="Namespaces to compare. At least one is required. Type to add custom values." /></>}
                required
                placeholder={nsValue.length === 0 ? 'Select or type...' : ''}
                {...err(fieldErrors, 'namespaces')}
              />
            )}
          />
        </Grid>
        <Grid size={{ xs: 12, sm: 4 }}>
          <Autocomplete
            multiple
            freeSolo
            options={availableSets}
            value={setValue}
            onChange={(_, newVal) => onChange('setNames', newVal.join(','))}
            renderInput={(params) => (
              <TextField
                {...params}
                label={<>Sets <HelpTip text="Set names to compare. Leave empty to scan all sets. Type to add custom values." /></>}
                placeholder={setValue.length === 0 ? 'All sets' : ''}
                {...err(fieldErrors, 'setNames')}
              />
            )}
          />
        </Grid>
        <Grid size={{ xs: 12, sm: 4 }}>
          <FormControl fullWidth size="small" {...(fieldErrors?.compareMode ? { error: true } : {})}>
            <InputLabel>Compare Mode</InputLabel>
            <Select
              label="Compare Mode"
              value={(options.compareMode as string) || 'MISSING_RECORDS'}
              onChange={(e) => onChange('compareMode', e.target.value)}
              endAdornment={<HelpTip text="QUICK_NAMESPACE: fast partition count. MISSING_RECORDS: digest-level. RECORDS_DIFFERENT: reads records. RECORD_DIFFERENCES: detailed diffs. FIND_OVERLAP: common records." />}
            >
              <MenuItem value="QUICK_NAMESPACE">Quick Namespace</MenuItem>
              <MenuItem value="MISSING_RECORDS">Missing Records</MenuItem>
              <MenuItem value="RECORDS_DIFFERENT">Records Different</MenuItem>
              <MenuItem value="RECORD_DIFFERENCES">Record Differences</MenuItem>
              <MenuItem value="FIND_OVERLAP">Find Overlap</MenuItem>
            </Select>
            {fieldErrors?.compareMode && <FormHelperText>{fieldErrors.compareMode}</FormHelperText>}
          </FormControl>
        </Grid>
        <Grid size={{ xs: 12, sm: 4 }}>
          <FormControl fullWidth size="small">
            <InputLabel>Action</InputLabel>
            <Select
              label="Action"
              value={(options.action as string) || 'SCAN'}
              onChange={(e) => onChange('action', e.target.value)}
              endAdornment={<HelpTip text="SCAN: find differences. TOUCH/READ: act on previous output. RERUN: re-check results. SCAN_TOUCH/SCAN_READ: scan then act." />}
            >
              <MenuItem value="SCAN">Scan</MenuItem>
              <MenuItem value="TOUCH">Touch</MenuItem>
              <MenuItem value="READ">Read</MenuItem>
              <MenuItem value="RERUN">Rerun</MenuItem>
              <MenuItem value="SCAN_TOUCH">Scan + Touch</MenuItem>
              <MenuItem value="SCAN_ASK">Scan + Ask</MenuItem>
              <MenuItem value="SCAN_READ">Scan + Read</MenuItem>
              <MenuItem value="CUSTOM">Custom</MenuItem>
              <MenuItem value="SCAN_CUSTOM">Scan + Custom</MenuItem>
            </Select>
          </FormControl>
        </Grid>
        <Grid size={{ xs: 12, sm: 4 }}>
          <TextField
            fullWidth label={<>Threads <HelpTip text="Parallel worker threads. 0 = auto-detect (one per CPU core). Default: 0." /></>}
            type="number"
            value={(options.threads as number) ?? 0}
            onChange={(e) => onChange('threads', Number(e.target.value))}
            slotProps={{ htmlInput: { min: 0 } }}
            {...err(fieldErrors, 'threads')}
          />
        </Grid>
        <Grid size={{ xs: 12, sm: 4 }}>
          <TextField
            fullWidth label={<>RPS Limit <HelpTip text="Max requests/second per thread per cluster. 0 = unlimited." /></>}
            type="number"
            value={(options.rps as number) ?? 0}
            onChange={(e) => onChange('rps', Number(e.target.value))}
            slotProps={{ htmlInput: { min: 0 } }}
            {...err(fieldErrors, 'rps')}
          />
        </Grid>
      </Grid>

      <Divider sx={{ my: 2.5 }} />
      <Typography variant="subtitle1" sx={{ fontWeight: 700 }} gutterBottom>Partitions &amp; Limits</Typography>
      <Grid container spacing={2}>
        <Grid size={{ xs: 6, sm: 3 }}>
          <TextField
            fullWidth label={<>Start Partition <HelpTip text="First partition to compare (0-4095). Default: 0." /></>}
            type="number"
            value={(options.startPartition as number) ?? 0}
            onChange={(e) => onChange('startPartition', Number(e.target.value))}
            slotProps={{ htmlInput: { min: 0, max: 4095 } }}
            {...err(fieldErrors, 'startPartition')}
          />
        </Grid>
        <Grid size={{ xs: 6, sm: 3 }}>
          <TextField
            fullWidth label={<>End Partition <HelpTip text="Last partition (exclusive). Default: 4096." /></>}
            type="number"
            value={(options.endPartition as number) ?? 4096}
            onChange={(e) => onChange('endPartition', Number(e.target.value))}
            slotProps={{ htmlInput: { min: 1, max: 4096 } }}
            {...err(fieldErrors, 'endPartition')}
          />
        </Grid>
        <Grid size={{ xs: 6, sm: 3 }}>
          <TextField
            fullWidth label={<>Difference Limit <HelpTip text="Stop after this many missing/different records. 0 = unlimited." /></>}
            type="number"
            value={(options.limit as number) ?? 0}
            onChange={(e) => onChange('limit', Number(e.target.value))}
            slotProps={{ htmlInput: { min: 0 } }}
            {...err(fieldErrors, 'limit')}
          />
        </Grid>
        <Grid size={{ xs: 6, sm: 3 }}>
          <TextField
            fullWidth label={<>Record Limit <HelpTip text="Max total records to compare. 0 = unlimited." /></>}
            type="number"
            value={(options.recordLimit as number) ?? 0}
            onChange={(e) => onChange('recordLimit', Number(e.target.value))}
            slotProps={{ htmlInput: { min: 0 } }}
            {...err(fieldErrors, 'recordLimit')}
          />
        </Grid>
        <Grid size={{ xs: 12, sm: 6 }}>
          <TextField
            fullWidth label={<>Partition List <HelpTip text="Explicit comma-separated partition IDs. Overrides start/end partition." /></>}
            value={(options.partitionList as string) || ''}
            onChange={(e) => onChange('partitionList', e.target.value)}
            placeholder="e.g. 0,1,2,100"
            {...err(fieldErrors, 'partitionList')}
          />
        </Grid>
      </Grid>

      <Divider sx={{ my: 2.5 }} />
      <DateRangeSection options={options} onChange={onChange} fieldErrors={fieldErrors} />

      <Divider sx={{ my: 2.5 }} />
      <Typography variant="subtitle1" sx={{ fontWeight: 700 }} gutterBottom>Files</Typography>
      <Grid container spacing={2}>
        <Grid size={{ xs: 12, sm: 6 }}>
          <TextField
            fullWidth label={<>Output File <HelpTip text="Path to CSV file for recording differences." /></>}
            value={(options.file as string) || ''}
            onChange={(e) => onChange('file', e.target.value)}
            placeholder="/path/to/output.csv"
            inputRef={outputFileInputRef}
            {...err(fieldErrors, 'file')}
          />
        </Grid>
        <Grid size={{ xs: 12, sm: 6 }}>
          <TextField
            fullWidth label={<>Input File <HelpTip text="Path to CSV file from a previous run. Used with RERUN, READ, TOUCH actions." /></>}
            value={(options.inputFile as string) || ''}
            onChange={(e) => onChange('inputFile', e.target.value)}
            placeholder="/path/to/input.csv"
            {...err(fieldErrors, 'inputFile')}
          />
        </Grid>
      </Grid>

      <Divider sx={{ my: 2.5 }} />
      <Typography variant="subtitle1" sx={{ fontWeight: 700 }} gutterBottom>Flags</Typography>
      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
        <FormControlLabel control={<Switch checked={!!options.quiet} onChange={(e) => onChange('quiet', e.target.checked)} size="small" />} label={<>Quiet <HelpTip text="Suppress progress output. Differences are still reported." /></>} />
        <FormControlLabel control={<Switch checked={!!options.console} onChange={(e) => onChange('console', e.target.checked)} size="small" />} label={<>Console <HelpTip text="Print differences to the console in addition to any output file." /></>} />
        <FormControlLabel control={<Switch checked={!!options.verbose} onChange={(e) => onChange('verbose', e.target.checked)} size="small" />} label={<>Verbose <HelpTip text="Enable verbose logging." /></>} />
        <FormControlLabel control={<Switch checked={!!options.debug} onChange={(e) => onChange('debug', e.target.checked)} size="small" />} label={<>Debug <HelpTip text="Enable debug mode. Turns on verbose and disables quiet." /></>} />
        <FormControlLabel control={<Switch checked={!!options.binsOnly} onChange={(e) => onChange('binsOnly', e.target.checked)} size="small" />} label={<>Bins Only <HelpTip text="Only list differing bin names, not full values." /></>} />
        <FormControlLabel control={<Switch checked={!!options.showMetadata} onChange={(e) => onChange('showMetadata', e.target.checked)} size="small" />} label={<>Show Metadata <HelpTip text="Include last-update-time and record size in output." /></>} />
        <FormControlLabel control={<Switch checked={!!options.sortMaps} onChange={(e) => onChange('sortMaps', e.target.checked)} size="small" />} label={<>Sort Maps <HelpTip text="Sort map bins before hashing to prevent false positives." /></>} />
        <FormControlLabel control={<Switch checked={!!options.metadataCompare} onChange={(e) => onChange('metadataCompare', e.target.checked)} size="small" />} label={<>Meta Compare <HelpTip text="Perform metadata comparison between clusters before the main scan." /></>} />
      </Box>
    </Paper>
  );
}
