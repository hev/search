package cmd

import (
	"fmt"
	"time"

	"github.com/hev/search/cli/internal/client"
	"github.com/hev/search/cli/internal/output"
)

// pollInterval is how often --wait polls an operation.
const pollInterval = 500 * time.Millisecond

// handleAccepted renders a 202 OperationAccepted and, when wait is set,
// polls the operation to a terminal state.
func handleAccepted(c *client.Client, op client.OperationAccepted, raw []byte, wait bool) error {
	if !wait {
		if output.IsJSON() {
			output.JSONRaw(raw)
			return nil
		}
		output.Statusf("Started %s on %s (operation_id %s, status %s).", op.Kind, op.Namespace, op.OperationID, op.Status)
		return nil
	}
	return waitForOp(c, op.OperationID)
}

// waitForOp polls an operation until it succeeds or fails.
func waitForOp(c *client.Client, id string) error {
	for {
		rec, raw, err := c.GetOperation(ctx(), id)
		if err != nil {
			return err
		}
		switch rec.Status {
		case "succeeded", "failed":
			if output.IsJSON() {
				output.JSONRaw(raw)
			} else if rec.Status == "failed" {
				msg := ""
				if rec.Error != nil {
					msg = ": " + *rec.Error
				}
				return fmt.Errorf("operation %s failed%s", id, msg)
			} else {
				output.Statusf("Operation %s (%s) succeeded.", id, rec.Kind)
			}
			return nil
		default:
			time.Sleep(pollInterval)
		}
	}
}
