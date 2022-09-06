with account as (
    SELECT 
        _id,
        name
        date,
        description,
        logoUrl,
        CAST(isLocked AS boolean) as isLocked,
        CAST(isFrozen AS boolean) as isFrozen,
        CAST(isEnabled AS boolean) as isEnabled,
        CAST(isBlocked AS boolean) as isBlocked,
        CAST(isActivated AS boolean) as isActivated,
        CAST(isSuspended AS boolean) as isSuspended,
        CAST(deleted AS boolean) as deleted,
        CAST(hasBranches AS double precision) as hasBranches,
        accountType,
        accountClass,
        accountStatus,
        summaryGL,
        cast(dateCreated AS {{ type_timestamp() }}) as dateCreated,
        accountCurrency,
        CAST(exchangeRate AS double precision) as exchangeRate,
        CAST(autorunEod AS boolean) as autorunEod,
        CAST(prePersistTransactions AS double precision) as prePersistTransactions,
        CAST(trackDenominations AS double precision) as trackDenominations,
        CAST(prepaidAccount AS double precision) as prepaidAccount,
        CAST(balance AS double precision) as balance,
        CAST(receiptImageSupported AS double precision) as receiptImageSupported, 
        countryCode,
        CAST(autoReversalSupported AS double precision) as autoReversalSupported,
        CAST(unsettledFunds AS double precision)  as unsettledFunds,
        CAST(notifyOnPayment AS double precision) as notifyOnPayment,
        CAST(chargeByChannel AS double precision) as chargeByChannel,
        CAST(autoClearCheques AS double precision) as autoClearCheques,
        sortCode,
        ownedBy,
        financeEmailContact,
        dataFormatters,
        callbackServiceCode,
        defaultReferenceField,
        reversalUser,
        CAST(orderTTL AS double precision) as orderTTL,
        webCheckoutUrl,
        callbackUser,
        themeFile,
        cast(dateModified AS {{ type_timestamp() }}) as dateModified,
        createdBy, 
        parentId
    FROM
        {{ source("billbox", "billbox_accounts") }}
)

SELECT * 
FROM account
