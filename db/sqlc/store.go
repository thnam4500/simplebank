package db

import (
	"context"
	"database/sql"
	"fmt"
)

type Store struct{
	*Queries
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{
		Queries: New(db),
		db:      db,
	}
}

func (store *Store) execTx(ctx context.Context, fn func(*Queries) error) error {
	tx, err := store.db.BeginTx(ctx, nil)

	if err != nil {
		return err
	}

	q := New(tx)
	err = fn(q)

	if err != nil {
		if rbError := tx.Rollback(); rbError != nil {
			return fmt.Errorf("tx error %v, rbError: %v", err, rbError)
		}
		return err
	}

	return tx.Commit()
}

type TransferTxParams struct {
	FromAccountID	int64 `json:"from_account_id"`
	ToAccountID		int64 `json:"to_account_id"`
	Amount 			int64 `json:"amount"`
}

type TransferResult struct{
	Transfer	Transfer `json:"transfer"`
	FromAccount Account	 `json:"from_account"`
	ToAccount  	Account	`json:"to_account"`
	FromEntry	Entry 	`json:"from_entry"`
	ToEntry     Entry    `json:"to_entry"`
}

var txKey = struct{}{}

func (store *Store) TransferTx(ctx context.Context, arg TransferTxParams) (TransferResult, error) {
	var result TransferResult

	err := store.execTx(ctx, func(q *Queries) error{
		var err error
		txName := ctx.Value(txKey)
		fmt.Println(txName,"create Transfer")
		result.Transfer, err = q.CreateTransfer(ctx, CreateTransferParams{
			FromAccountID: arg.FromAccountID,
			ToAccountID:   arg.ToAccountID,
			Amount:        arg.Amount,
		})
		if err != nil {
			return err
		}
		fmt.Println(txName,"create Entry 1")
		result.FromEntry, err = q.CreateEntry(ctx, CreateEntryParams{
			AccountID: arg.FromAccountID,
			Amount:    -arg.Amount,
		})
		if err!= nil {
			return err
		}
	
		fmt.Println(txName,"create Entry 2")
		result.ToEntry, err = q.CreateEntry(ctx, CreateEntryParams{
			AccountID: arg.ToAccountID,
			Amount:    arg.Amount,
		})


		if arg.FromAccountID > arg.ToAccountID{
			result.FromAccount, err = q.AddAccountBalance(ctx, AddAccountBalanceParams{
				ID: arg.FromAccountID,
				Amount: -arg.Amount,
			})
			if err!= nil {
				return err
			}


			
			result.ToAccount, err = q.AddAccountBalance(ctx, AddAccountBalanceParams{
				ID: arg.ToAccountID,
				Amount: arg.Amount,
			})
			if err!= nil {
				return err
			}
		}else{
			result.ToAccount, err = q.AddAccountBalance(ctx, AddAccountBalanceParams{
				ID: arg.ToAccountID,
				Amount: arg.Amount,
			})
			if err!= nil {
				return err
			}
			result.FromAccount, err = q.AddAccountBalance(ctx, AddAccountBalanceParams{
				ID: arg.FromAccountID,
				Amount: -arg.Amount,
			})
			if err!= nil {
				return err
			}
		}
		

		return nil
	})

	return result, err
}