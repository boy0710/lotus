//go:build cgo
// +build cgo

package ffiwrapper

import (
	"context"

	"go.opencensus.io/trace"
	"golang.org/x/xerrors"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/network"
	ffiproof "github.com/filecoin-project/specs-actors/v5/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v7/actors/runtime/proof"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/lotus/chain/actors/policy"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

func (sb *Sealer) GenerateWinningPoSt(ctx context.Context, minerID abi.ActorID, sectorInfo []proof.ExtendedSectorInfo, randomness abi.PoStRandomness, poStEpoch abi.ChainEpoch, version network.Version) ([]proof.PoStProof, error) {
	randomness[31] &= 0x3f
	takeSectorKey := func(ssi proof.ExtendedSectorInfo) cid.Cid {
		// no update
		if ssi.SectorKey == nil {
			return ssi.SealedCID
		}
		lb := policy.GetWinningPoStSectorSetLookback(version)
		// update not yet in lookback state
		if ssi.Activation+lb > poStEpoch {
			return *ssi.SectorKey
		}
		return ssi.SealedCID
	}
	privsectors, skipped, done, err := sb.pubExtendedSectorToPriv(ctx, minerID, sectorInfo, nil, abi.RegisteredSealProof.RegisteredWinningPoStProof, takeSectorKey) // TODO: FAULTS?
	if err != nil {
		return nil, err
	}
	defer done()
	if len(skipped) > 0 {
		return nil, xerrors.Errorf("pubSectorToPriv skipped sectors: %+v", skipped)
	}

	return ffi.GenerateWinningPoSt(minerID, privsectors, randomness)
}

func (sb *Sealer) GenerateWindowPoSt(ctx context.Context, minerID abi.ActorID, sectorInfo []proof.ExtendedSectorInfo, randomness abi.PoStRandomness) ([]proof.PoStProof, []abi.SectorID, error) {
	log.Errorf("Generate Window PoST cgo sealer!")
	randomness[31] &= 0x3f
	takeSealed := func(ssi proof.ExtendedSectorInfo) cid.Cid {
		return ssi.SealedCID
	}
	privsectors, skipped, done, err := sb.pubExtendedSectorToPriv(ctx, minerID, sectorInfo, nil, abi.RegisteredSealProof.RegisteredWindowPoStProof, takeSealed)
	if err != nil {
		return nil, nil, xerrors.Errorf("gathering sector info: %w", err)
	}
	log.Errorf("got private sectors")

	defer done()

	if len(skipped) > 0 {
		return nil, skipped, xerrors.Errorf("pubSectorToPriv skipped some sectors")
	}

	proof, faulty, err := ffi.GenerateWindowPoSt(minerID, privsectors, randomness)
	log.Errorf("generated window post")

	var faultyIDs []abi.SectorID
	for _, f := range faulty {
		faultyIDs = append(faultyIDs, abi.SectorID{
			Miner:  minerID,
			Number: f,
		})
	}
	log.Errorf("coming back")
	return proof, faultyIDs, err
}

func (sb *Sealer) pubExtendedSectorToPriv(ctx context.Context, mid abi.ActorID, sectorInfo []proof.ExtendedSectorInfo, faults []abi.SectorNumber, rpt func(abi.RegisteredSealProof) (abi.RegisteredPoStProof, error), proofCID func(proof.ExtendedSectorInfo) cid.Cid) (ffi.SortedPrivateSectorInfo, []abi.SectorID, func(), error) {
	fmap := map[abi.SectorNumber]struct{}{}
	for _, fault := range faults {
		fmap[fault] = struct{}{}
	}

	var doneFuncs []func()
	done := func() {
		for _, df := range doneFuncs {
			df()
		}
	}

	var skipped []abi.SectorID
	var out []ffi.PrivateSectorInfo
	for _, s := range sectorInfo {
		if _, faulty := fmap[s.SectorNumber]; faulty {
			continue
		}

		sid := storage.SectorRef{
			ID:        abi.SectorID{Miner: mid, Number: s.SectorNumber},
			ProofType: s.SealProof,
		}
		proveUpdate := s.SectorKey != nil && proofCID(s) == s.SealedCID
		var cache string
		var sealed string
		if proveUpdate {
			log.Errorf("Posting over updated sector for sector id: %d", s.SectorNumber)
			paths, d, err := sb.sectors.AcquireSector(ctx, sid, storiface.FTUpdateCache|storiface.FTUpdate, 0, storiface.PathStorage)
			if err != nil {
				log.Warnw("failed to acquire FTUpdateCache and FTUpdate of sector, skipping", "sector", sid.ID, "error", err)
				skipped = append(skipped, sid.ID)
				continue
			}
			doneFuncs = append(doneFuncs, d)
			cache = paths.UpdateCache
			sealed = paths.Update
		} else {
			log.Errorf("Posting over sector key sector for sector id: %d", s.SectorNumber)
			paths, d, err := sb.sectors.AcquireSector(ctx, sid, storiface.FTCache|storiface.FTSealed, 0, storiface.PathStorage)
			if err != nil {
				log.Warnw("failed to acquire FTCache and FTSealed of sector, skipping", "sector", sid.ID, "error", err)
				skipped = append(skipped, sid.ID)
				continue
			}
			doneFuncs = append(doneFuncs, d)
			cache = paths.Cache
			sealed = paths.Sealed
		}

		postProofType, err := rpt(s.SealProof)
		if err != nil {
			done()
			return ffi.SortedPrivateSectorInfo{}, nil, nil, xerrors.Errorf("acquiring registered PoSt proof from sector info %+v: %w", s, err)
		}

		ffiInfo := ffiproof.SectorInfo{
			SealProof:    s.SealProof,
			SectorNumber: s.SectorNumber,
			SealedCID:    proofCID(s),
		}
		out = append(out, ffi.PrivateSectorInfo{
			CacheDirPath:     cache,
			PoStProofType:    postProofType,
			SealedSectorPath: sealed,
			SectorInfo:       ffiInfo,
		})
	}

	return ffi.NewSortedPrivateSectorInfo(out...), skipped, done, nil
}

var _ Verifier = ProofVerifier

type proofVerifier struct{}

var ProofVerifier = proofVerifier{}

func (proofVerifier) VerifySeal(info proof.SealVerifyInfo) (bool, error) {
	return ffi.VerifySeal(info)
}

func (proofVerifier) VerifyAggregateSeals(aggregate proof.AggregateSealVerifyProofAndInfos) (bool, error) {
	return ffi.VerifyAggregateSeals(aggregate)
}

func (proofVerifier) VerifyReplicaUpdate(update proof.ReplicaUpdateInfo) (bool, error) {
	return ffi.SectorUpdate.VerifyUpdateProof(update)
}

func (proofVerifier) VerifyWinningPoSt(ctx context.Context, info proof.WinningPoStVerifyInfo, poStEpoch abi.ChainEpoch, version network.Version) (bool, error) {
	info.Randomness[31] &= 0x3f
	_, span := trace.StartSpan(ctx, "VerifyWinningPoSt")
	defer span.End()

	// Transform winning post verify to use the correct sealed cid with respect to the lookback
	ffiInfo := ffiproof.WinningPoStVerifyInfo{
		Randomness:        info.Randomness,
		Proofs:            info.Proofs,
		Prover:            info.Prover,
		ChallengedSectors: ffiSectorInfosTakeSectorKeyUntilCutoff(info.ChallengedSectors, poStEpoch, policy.GetWinningPoStSectorSetLookback(version)),
	}

	return ffi.VerifyWinningPoSt(ffiInfo)
}

func (proofVerifier) VerifyWindowPoSt(ctx context.Context, info proof.WindowPoStVerifyInfo) (bool, error) {
	info.Randomness[31] &= 0x3f
	_, span := trace.StartSpan(ctx, "VerifyWindowPoSt")
	defer span.End()

	return ffi.VerifyWindowPoSt(info)
}

func (proofVerifier) GenerateWinningPoStSectorChallenge(ctx context.Context, proofType abi.RegisteredPoStProof, minerID abi.ActorID, randomness abi.PoStRandomness, eligibleSectorCount uint64) ([]uint64, error) {
	randomness[31] &= 0x3f
	return ffi.GenerateWinningPoStSectorChallenge(proofType, minerID, randomness, eligibleSectorCount)
}

// For non-upgraded sectors take the sealed cid
// For upgraded sectors take the sector key as sealed cid until
//   sector activation + cutoff is in the past, i.e. less than current epoch
func ffiSectorInfosTakeSectorKeyUntilCutoff(infos []proof.ExtendedSectorInfo, current, cutoff abi.ChainEpoch) []ffiproof.SectorInfo {
	ffiInfos := make([]ffiproof.SectorInfo, len(infos))
	for i, ssi := range infos {
		sealed := ssi.SealedCID
		// lookback state is at current - cutoff. Once this is activation or greater lookback state includes upgrade
		if ssi.SectorKey != nil && cutoff+ssi.Activation > current {
			sealed = *ssi.SectorKey
		}

		ffiInfos[i] = ffiproof.SectorInfo{
			SealProof:    ssi.SealProof,
			SectorNumber: ssi.SectorNumber,
			SealedCID:    sealed,
		}
	}
	return ffiInfos
}
