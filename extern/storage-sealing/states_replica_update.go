package sealing

import (
	"bytes"
	"fmt"

	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	statemachine "github.com/filecoin-project/go-statemachine"
	api "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"golang.org/x/xerrors"
)

func (m *Sealing) handleReplicaUpdate(ctx statemachine.Context, sector SectorInfo) error {
	if err := checkPieces(ctx.Context(), m.maddr, sector, m.Api); err != nil { // Sanity check state
		switch err.(type) {
		case *ErrApi:
			log.Errorf("handleReplicaUpdate: api error, not proceeding: %+v", err)
			return nil
		case *ErrInvalidDeals:
			log.Warnf("invalid deals in sector %d: %v", sector.SectorNumber, err)
			return ctx.Send(SectorInvalidDealIDs{Return: RetPreCommit1})
		case *ErrExpiredDeals: // Probably not much we can do here, maybe re-pack the sector?
			return ctx.Send(SectorDealsExpired{xerrors.Errorf("expired dealIDs in sector: %w", err)})
		default:
			return xerrors.Errorf("checkPieces sanity check error: %w", err)
		}
	}
	log.Errorf("RU %d", sector.SectorNumber)
	out, err := m.sealer.ReplicaUpdate(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorType, sector.SectorNumber), sector.pieceInfos())
	if err != nil {
		// XXX error handling events and states
		fmt.Printf("err: %s, out: %v\n", err, out)
		panic("bad replica update")
	}
	return ctx.Send(SectorReplicaUpdate{
		Out: out,
	})
}

func (m *Sealing) handleProveReplicaUpdate1(ctx statemachine.Context, sector SectorInfo) error {
	if sector.UpdateSealed == nil || sector.UpdateUnsealed == nil {
		return xerrors.Errorf("invalid sector %d with nil UpdateSealed or UpdateUnsealed output", sector.SectorNumber)
	}
	if sector.CommR == nil {
		return xerrors.Errorf("invalid sector %d with nil CommR", sector.SectorNumber)
	}
	log.Errorf("PR1 %d", sector.SectorNumber)
	vanillaProofs, err := m.sealer.ProveReplicaUpdate1(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorType, sector.SectorNumber), *sector.CommR, *sector.UpdateSealed, *sector.UpdateUnsealed)
	if err != nil {
		// XXX error handling events and states
		panic("bad prove replica update 1")
	}
	return ctx.Send(SectorProveReplicaUpdate1{
		Out: vanillaProofs,
	})
}

func (m *Sealing) handleProveReplicaUpdate2(ctx statemachine.Context, sector SectorInfo) error {
	if sector.UpdateSealed == nil || sector.UpdateUnsealed == nil {
		return xerrors.Errorf("invalid sector %d with nil UpdateSealed or UpdateUnsealed output", sector.SectorNumber)
	}
	if sector.CommR == nil {
		return xerrors.Errorf("invalid sector %d with nil CommR", sector.SectorNumber)
	}
	if sector.ProveReplicaUpdate1Out == nil {
		return xerrors.Errorf("invalid sector %d with nil ProveReplicaUpdate1 output", sector.SectorNumber)
	}
	proof, err := m.sealer.ProveReplicaUpdate2(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorType, sector.SectorNumber), *sector.CommR, *sector.UpdateSealed, *sector.UpdateUnsealed, sector.ProveReplicaUpdate1Out)
	if err != nil {
		// XXX error handling events and states
		panic("bad prove replica update 2")
	}
	return ctx.Send(SectorProveReplicaUpdate2{
		Proof: proof,
	})
}

func (m *Sealing) handleSubmitReplicaUpdate(ctx statemachine.Context, sector SectorInfo) error {

	// XXX use config for various collateral things

	// cfg, err := m.getConfig()
	// if err != nil {
	// 	return xerrors.Errorf("getting config: %w", err)
	// }
	tok, _, err := m.Api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("handleSubmitReplicaUpdate: api error, not proceeding: %+v", err)
		return nil
	}

	// XXX: check replica update

	sl, err := m.Api.StateSectorPartition(ctx.Context(), m.maddr, sector.SectorNumber, tok)
	if err != nil {
		log.Errorf("handleSubmitReplicaUpdate: api error, not proceeding: %+v", err)
		return nil
	}
	updateProof, err := sector.SectorType.RegisteredUpdateProof()
	if err != nil {
		return xerrors.Errorf("failed to get update proof type from seal proof: %w", err)
	}
	enc := new(bytes.Buffer)
	params := &miner.ProveReplicaUpdatesParams{
		Updates: []miner.ReplicaUpdate{
			{
				SectorID:           sector.SectorNumber,
				Deadline:           sl.Deadline,
				Partition:          sl.Partition,
				NewSealedSectorCID: *sector.UpdateSealed,
				Deals:              sector.dealIDs(),
				UpdateProofType:    updateProof,
				ReplicaProof:       sector.ReplicaUpdateProof,
			},
		},
	}
	if err := params.MarshalCBOR(enc); err != nil {
		return xerrors.Errorf("failed to serialize update replica params: %w", err)
	}

	// XXX fees need to be better handled
	mi, err := m.Api.StateMinerInfo(ctx.Context(), m.maddr, tok)
	if err != nil {
		log.Errorf("handleSubmitReplicaUpdate: api error, not proceeding: %+v", err)
		return nil
	}

	from, _, err := m.addrSel(ctx.Context(), mi, api.CommitAddr, big.Zero(), big.Zero())
	if err != nil {
		return xerrors.Errorf("no good address to send replica update message from: %w", err)
	}
	// XXX fees need to be better handled
	mcid, err := m.Api.SendMsg(ctx.Context(), from, m.maddr, miner.Methods.ProveReplicaUpdates, big.Zero(), big.Int(m.feeCfg.MaxCommitGasFee), enc.Bytes())

	return ctx.Send(SectorReplicaUpdateSubmitted{Message: mcid})
}

func (m *Sealing) handleReplicaUpdateWait(ctx statemachine.Context, sector SectorInfo) error {
	if sector.ReplicaUpdateMessage == nil {
		panic("handling error states and events")
	}

	mw, err := m.Api.StateWaitMsg(ctx.Context(), *sector.ReplicaUpdateMessage)
	if err != nil {
		panic("deal with failures")
	}

	switch mw.Receipt.ExitCode {
	case exitcode.Ok:
		//expected
	case exitcode.SysErrInsufficientFunds:
		fallthrough
	case exitcode.SysErrOutOfGas:
		// gas estimator was wrong or out of funds
		panic("should do a retry")
	default:
		panic("unrecovereable failure")
	}
	si, err := m.Api.StateSectorGetInfo(ctx.Context(), m.maddr, sector.SectorNumber, mw.TipSetTok)
	if err != nil {
		panic("error getting sector info")
	}
	if si == nil {
		panic("failure doing replica update, sector no longe exists")
	}

	if !si.SealedCID.Equals(*sector.UpdateSealed) {
		log.Errorf("mismatch of expected onchain sealed cid after replica update, expected %s got %s", sector.UpdateSealed, si.SealedCID)
		panic("fail here")
	}
	return ctx.Send(SectorReplicaUpdateLanded{})
}

func (m *Sealing) handleFinalizeReplicaUpdate(ctx statemachine.Context, sector SectorInfo) error {
	return ctx.Send(SectorFinalized{})
}
