package arsyncer

import "github.com/duke-git/lancet/v2/slice"

func (s *Syncer) runJobs() {
	s.scheduler.Every(5).Seconds().SingletonMode().Do(s.updateBlockHashList)
	s.scheduler.Every(1).Minute().SingletonMode().Do(s.updatePeers)
	s.scheduler.StartAsync()
}

func (s *Syncer) updateBlockHashList() {
	if s.blockIdxs.EndHeight-s.curHeight > s.stableDistance/2+1 {
		return
	}

	idxs, err := GetBlockIdxs(s.curHeight, s.arClient)
	if err != nil {
		log.Error("get blockIdxs failed", "err", err)
		return
	}
	s.blockIdxs = idxs
	log.Debug("update block hash_list sucess", "startHeight", idxs.StartHeight, "endHeight", idxs.EndHeight)
}

func (s *Syncer) updatePeers() {
	peers, err := s.arClient.GetPeers()
	if err != nil {
		return
	}
	if len(peers) == 0 {
		return
	}
	// update
	if s.importantPeers != nil && len(s.importantPeers) > 0 {
		peers = slice.Without(peers, s.importantPeers...)
		peers = append(s.importantPeers, peers...)
	}
	s.peers = peers
}
func (s *Syncer) clearJobs() {
	s.scheduler.Clear()
}
