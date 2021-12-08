package sealing

import (
	"context"

	"github.com/filecoin-project/go-statemachine"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	market7 "github.com/filecoin-project/specs-actors/v7/actors/builtin/market"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
)

func (m *Sealing) IsMarkedForUpgrade(id abi.SectorNumber) bool {
	m.upgradeLk.Lock()
	_, found := m.toUpgrade[id]
	m.upgradeLk.Unlock()
	return found
}

func (m *Sealing) MarkForUpgrade(ctx context.Context, id abi.SectorNumber) error {
	m.upgradeLk.Lock()
	defer m.upgradeLk.Unlock()

	// _, found := m.toUpgrade[id]
	// if found {
	// 	return xerrors.Errorf("sector %d already marked for upgrade", id)
	// }

	si, err := m.GetSectorInfo(id)
	if err != nil {
		return xerrors.Errorf("getting sector info: %w", err)
	}

	if si.State != Proving {
		return xerrors.Errorf("can't mark sectors not in the 'Proving' state for upgrade")
	}

	if len(si.Pieces) != 1 {
		return xerrors.Errorf("not a committed-capacity sector, expected 1 piece")
	}

	if si.Pieces[0].DealInfo != nil {
		return xerrors.Errorf("not a committed-capacity sector, has deals")
	}

	tok, head, err := m.Api.ChainHead(ctx)
	if err != nil {
		return xerrors.Errorf("couldnt get chain head: %w", err)
	}
	onChainInfo, err := m.Api.StateSectorGetInfo(ctx, m.maddr, id, tok)
	if err != nil {
		return xerrors.Errorf("failed to read sector on chain info: %w", err)
	}

	if onChainInfo.Expiration-head < market7.DealMinDuration {
		return xerrors.Errorf("pointless to upgrade sector %d, expiration %d is less than a min deal duration away from current epoch."+
			"Upgrade expiration before marking for upgrade", id, onChainInfo.Expiration)
	}

	log.Errorf("updating sector number %d", id)
	// m.toUpgrade[id] = struct{}{}
	m.sectors.Send(uint64(id), SectorStartCCUpdate{})

	return nil
}

func (m *Sealing) handleCCUpdate(ctx statemachine.Context, sector SectorInfo) error {
	log.Errorf("handle CC %d", sector.SectorNumber)
	return m.sectors.Send(uint64(sector.SectorNumber), SectorStart{})
}

func (m *Sealing) tryUpgradeSector(ctx context.Context, params *miner.SectorPreCommitInfo) big.Int {
	if len(params.DealIDs) == 0 {
		return big.Zero()
	}
	replace := m.maybeUpgradableSector()
	if replace != nil {
		loc, err := m.Api.StateSectorPartition(ctx, m.maddr, *replace, nil)
		if err != nil {
			log.Errorf("error calling StateSectorPartition for replaced sector: %+v", err)
			return big.Zero()
		}

		params.ReplaceCapacity = true
		params.ReplaceSectorNumber = *replace
		params.ReplaceSectorDeadline = loc.Deadline
		params.ReplaceSectorPartition = loc.Partition

		log.Infof("replacing sector %d with %d", *replace, params.SectorNumber)

		ri, err := m.Api.StateSectorGetInfo(ctx, m.maddr, *replace, nil)
		if err != nil {
			log.Errorf("error calling StateSectorGetInfo for replaced sector: %+v", err)
			return big.Zero()
		}
		if ri == nil {
			log.Errorf("couldn't find sector info for sector to replace: %+v", replace)
			return big.Zero()
		}

		if params.Expiration < ri.Expiration {
			// TODO: Some limit on this
			params.Expiration = ri.Expiration
		}

		return ri.InitialPledge
	}

	return big.Zero()
}

func (m *Sealing) maybeUpgradableSector() *abi.SectorNumber {
	m.upgradeLk.Lock()
	defer m.upgradeLk.Unlock()
	for number := range m.toUpgrade {
		// TODO: checks to match actor constraints

		// this one looks good
		/*if checks */
		{
			delete(m.toUpgrade, number)
			return &number
		}
	}

	return nil
}
