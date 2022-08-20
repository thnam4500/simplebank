package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransferTx(t *testing.T){
	store := NewStore(testDB)

	account1 := createRandomAccount(t)
	account2 := createRandomAccount(t)
	fmt.Println(">> BEFORE:",account1.Balance, account2.Balance)

	n := 2
	amount := int64(10)

	errs := make(chan error)
	results := make(chan TransferResult)

	for i:=0; i<n; i++ {
		txName := fmt.Sprintf("tx %d", i+1)
		go func(){
			ctx := context.WithValue(context.Background(),txKey, txName)
			result, err := store.TransferTx(ctx,TransferTxParams{
				FromAccountID: account1.ID,
				ToAccountID: account2.ID,
				Amount: amount,
			})
			errs <- err
			results <- result
		}()
	}

	existed := make(map[int]bool)

	for i:=0; i<n; i++ {
		err := <-errs
		require.NoError(t, err)

		result := <-results
		require.NotEmpty(t, result)

		transfer := result.Transfer
		require.NotEmpty(t,transfer)
		require.Equal(t, amount, transfer.Amount)
		require.Equal(t, account1.ID, transfer.FromAccountID)
		require.Equal(t, account2.ID, transfer.ToAccountID)
		require.NotZero(t, transfer.ID)
		require.NotZero(t, transfer.CreatedAt)

		_, err = store.GetTransfer(context.Background(), transfer.ID)
		require.NoError(t, err)

		fromEntry := result.FromEntry
		require.NotEmpty(t, fromEntry)
		require.Equal(t, account1.ID, fromEntry.AccountID)
		require.Equal(t, -amount, fromEntry.Amount)

		_, err = store.GetEntry(context.Background(), fromEntry.ID)
		require.NoError(t, err)

		toEntry := result.ToEntry
		require.NotEmpty(t,toEntry)
		require.Equal(t, account2.ID, toEntry.AccountID)
		require.Equal(t, amount, toEntry.Amount)

		_, err = store.GetEntry(context.Background(), toEntry.ID)
		require.NoError(t, err)

		// Check account
		fromAccount := result.FromAccount
		require.NotEmpty(t, fromAccount)
		require.Equal(t, account1.ID, fromAccount.ID)

		// _, err = store.GetAccount(context.Background(), fromAccount.ID)
		// require.NoError(t, err)

		toAccount := result.ToAccount
		require.NotEmpty(t, toAccount)
		require.Equal(t, account2.ID, toAccount.ID)

		// _, err = store.GetAccount(context.Background(), toAccount.ID)
		// require.Error(t, err)

		// Check account's balance
		fmt.Println(">> TX:", fromAccount.Balance, toAccount.Balance)

		diff1 := account1.Balance - fromAccount.Balance
		diff2 := toAccount.Balance - account2.Balance

		require.Equal(t, diff1, diff2)
		require.True(t, diff1>0)
		require.True(t, diff1%amount == 0)

		k := int(diff1/amount)
		require.True(t, k>=1 && k<=n)
		require.NotContains(t, existed,k)
		existed[k] = true
	}

	updateAccount1, err := testQueries.GetAccount(context.Background(),account1.ID)
	require.NoError(t, err)
	require.Equal(t, updateAccount1.Balance, account1.Balance - int64(n)*amount)

	updateAccount2, err := testQueries.GetAccount(context.Background(),account2.ID)
	require.NoError(t, err)
	require.Equal(t, updateAccount2.Balance, account2.Balance + int64(n)*amount)
	fmt.Println(">> AFTER:",updateAccount1.Balance, updateAccount2.Balance)
}

func TestTransferTxDeadlock(t *testing.T){
	store := NewStore(testDB)

	account1 := createRandomAccount(t)
	account2 := createRandomAccount(t)
	fmt.Println(">> BEFORE:",account1.Balance, account2.Balance)

	n := 10
	amount := int64(10)

	errs := make(chan error)

	for i:=0; i<n; i++ {
		fromAccountID := account1.ID
		toAccountID := account2.ID

		if i%2 == 1 {
			toAccountID = account1.ID
			fromAccountID = account2.ID
		}
		go func(){

			_, err := store.TransferTx(context.Background(),TransferTxParams{
				FromAccountID: fromAccountID,
				ToAccountID: toAccountID,
				Amount: amount,
			})
			errs <- err
		}()
	}

	for i:=0; i<n; i++ {
		err := <-errs
		require.NoError(t, err)
	}
	
	updateAccount1, err := testQueries.GetAccount(context.Background(),account1.ID)
	require.NoError(t, err)
	require.Equal(t, updateAccount1.Balance, account1.Balance)

	updateAccount2, err := testQueries.GetAccount(context.Background(),account2.ID)
	require.NoError(t, err)
	require.Equal(t, updateAccount2.Balance, account2.Balance)
}