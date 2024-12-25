Encryptionkey=aws/secretsmanager
SecretName=pg-redshift-secret
KeyValues:
    key-username
    value-pgawsuser

    key-password
    value-pgawsuser

    key-engine
    value-redshift

    key-host
    value-pg-redshift-cluster.cangtoce16mw.us-east-1.redshift.amazonaws.com

    key-port
    value-5439

    key-dbClusterIdentifier
    value-pg-redshift-cluster