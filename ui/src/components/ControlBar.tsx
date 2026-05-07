import { Button, Chip, Stack } from '@mui/material';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import StopIcon from '@mui/icons-material/Stop';
import RestartAltIcon from '@mui/icons-material/RestartAlt';

interface ControlBarProps {
  state: string;
  onStart: () => void;
  onStop: () => void;
  onReset: () => void;
}

function stateColor(state: string): 'primary' | 'success' | 'error' | 'default' {
  switch (state) {
    case 'RUNNING': return 'primary';
    case 'COMPLETE': return 'success';
    case 'ERROR': return 'error';
    default: return 'default';
  }
}

export default function ControlBar({ state, onStart, onStop, onReset }: ControlBarProps) {
  const isRunning = state === 'RUNNING';
  const isDone = state === 'COMPLETE' || state === 'ERROR';

  return (
    <Stack direction="row" spacing={1} sx={{ alignItems: 'center' }}>
      <Chip label={state} color={stateColor(state)} size="small" sx={{ fontWeight: 600 }} />
      <Button
        variant="contained"
        color="success"
        startIcon={<PlayArrowIcon />}
        onClick={onStart}
        disabled={isRunning}
        size="small"
      >
        Start
      </Button>
      <Button
        variant="contained"
        color="error"
        startIcon={<StopIcon />}
        onClick={onStop}
        disabled={!isRunning}
        size="small"
      >
        Stop
      </Button>
      {isDone && (
        <Button variant="outlined" startIcon={<RestartAltIcon />} onClick={onReset} size="small" sx={{ color: '#fff', borderColor: 'rgba(255,255,255,0.5)' }}>
          Reset
        </Button>
      )}
    </Stack>
  );
}
