public class Data {
    // NB In this code PROD database server means the production RMS server i.e. RAT-SQL-RMS
    // Supply overrides for all IP based values if DBs failover
    // Till we figure out how to cope with it
    //
    public static final String PROD_IP_ADDRESS_DEFAULT = "10.200.205.166";
    public static final String TEST_IP_ADDRESS_DEFAULT = "10.200.204.34";
    public static final String PROD_SQL_SERVER_TEMP_BAK_DIR_SMB = "smb://10.200.205.166/x$/TempBakDir/";
    public static final String TEST_SQL_SERVER_TEMP_BAK_DIR_SMB = "smb://10.200.204.34/x$/TempBakDir/";
    // For local testing, can override these via VM arguments -DMYPC_HOSTNAME="YOUR PC HOSTNAME HERE" etc
    public static final String MYPC_IP_ADDRESS_DEFAULT = "10.200.205.164";
    public static final String MYPC_HOSTNAME_DEFAULT = "localhost";
    public static final String UNC_PATH_MYPC_DEFAULT = "\\\\"+MYPC_HOSTNAME_DEFAULT+"\\c$\\/Program Files\\Microsoft SQL Server\\MSSQL11.SQLEXPRESS\\MSSQL\\DATA";
    public static final String DEFAULT_UNIVERSAL_SQL_SERVER_MDF_DATA_DIR = "\\\\RAT-SQL-RMS\\x$\\MSSQL11.MSSQLSERVER\\MSSQL\\DATA";
    public static final String DEFAULT_LOCAL_MDF_DIR_ON_SQL_SERVER = "x:\\MSSQL11.MSSQLSERVER\\MSSQL\\DATA\\";
    public static final String DEFAULT_LOCAL_LDF_DIR_ON_SQL_SERVER = "y:\\MSSQL11.MSSQLSERVER\\MSSQL\\DATA\\";
    public static final String DEFAULT_LOCAL_SQL_SERVER_TEMP_BAK_DIR = "X:/TempBakDir";
    public static final String UNC_PATH_PROD_DEFAULT = "\\\\RAT-SQL-RMS\\x$\\MSSQL11.MSSQLSERVER\\MSSQL\\DATA";
    public static final String UNC_PATH_TEST_DEFAULT = "\\\\RAT-SQL-APPS-TEST\\x$\\MSSQL11.MSSQLSERVER\\MSSQL\\Data";
    public static final String SMB_URL_PROD_DEFAULT = "smb://10.200.205.166/x$/MSSQL11.MSSQLSERVER/MSSQL/DATA";
    public static final String SMB_URL_TEST_DEFAULT = "smb://10.200.204.34/x$/MSSQL11.MSSQLSERVER/MSSQL/Data";
    public static final String SMB_URL_MYPC_DEFAULT = "smb://CH-E81-RAT024/C$/temp/dest/";
    public static final String TEST_SQL_SERVER_MDF_DATA_DIR = "\\\\RAT-SQL-APPS-TEST\\X$\\MSSQL11.MSSQLSERVER\\MSSQL\\Data";
}
