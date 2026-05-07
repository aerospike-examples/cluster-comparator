import { Button, Box } from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import ClusterCard from './ClusterCard';
import type { ClusterConfig } from '../api';

interface ConnectionPanelProps {
  options: Record<string, unknown>;
  onChange: (key: string, value: unknown) => void;
  fieldErrors?: Record<string, string>;
}

const EMPTY_CLUSTER: ClusterConfig = {
  hostName: '',
  clusterName: '',
  userName: '',
  password: '',
  authMode: 'INTERNAL',
  useServicesAlternate: false,
};

export default function ConnectionPanel({ options, onChange, fieldErrors }: ConnectionPanelProps) {
  const clusters = (options.clusters as ClusterConfig[] | undefined) || [{ ...EMPTY_CLUSTER }, { ...EMPTY_CLUSTER }];
  const clustersError = fieldErrors?.clusters;

  const updateCluster = (index: number, cluster: ClusterConfig) => {
    const next = [...clusters];
    next[index] = cluster;
    onChange('clusters', next);
  };

  const removeCluster = (index: number) => {
    onChange('clusters', clusters.filter((_, i) => i !== index));
  };

  const addCluster = () => {
    onChange('clusters', [...clusters, { ...EMPTY_CLUSTER }]);
  };

  return (
    <Box>
      {clusters.map((cluster, idx) => (
        <ClusterCard
          key={idx}
          index={idx}
          cluster={cluster}
          onChange={(c) => updateCluster(idx, c)}
          onRemove={() => removeCluster(idx)}
          canRemove={clusters.length > 2}
          hostError={clustersError && !cluster.hostName?.trim() ? clustersError : undefined}
        />
      ))}
      <Button variant="outlined" startIcon={<AddIcon />} onClick={addCluster} fullWidth sx={{ mt: 1, borderStyle: 'dashed', py: 1.2 }}>
        Add Cluster
      </Button>
    </Box>
  );
}
