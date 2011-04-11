//     ScriptSqlConfig - Generate a script of common SQL Server configration options and objects
//
//     Copyright (c) 2011 scaleSQL Consulting, LLC
//
//    Definitions
//    ===========
//    The terms "reproduce," "reproduction," "derivative works," and "distribution" have the same 
//        meaning here as under U.S. copyright law. 
//    A "contribution" is the original software, or any additions or changes to the software. 
//    A "contributor" is any person that distributes its contribution under this license. 
//    "Licensed patents" are a contributor's patent claims that read directly on its contribution.
//
//    Grant of Rights
//    ===============
//    (A) Copyright Grant- Subject to the terms of this license, including the license conditions and 
//        limitations in section 3, each contributor grants you a non-exclusive, worldwide, royalty-free 
//        copyright license to reproduce its contribution, prepare derivative works of its contribution, 
//        and distribute its contribution or any derivative works that you create.
//    (B) Patent Grant- Subject to the terms of this license, including the license conditions and 
//        limitations in section 3, each contributor grants you a non-exclusive, worldwide, 
//        royalty-free license under its licensed patents to make, have made, use, sell, offer for sale, 
//        import, and/or otherwise dispose of its contribution in the software or derivative works of 
//        the contribution in the software.
//
//    Conditions and Limitations
//    ==========================
//    (A) No Trademark License- This license does not grant you rights to use any contributors' 
//        name, logo, or trademarks. 
//    (B) If you bring a patent claim against any contributor over patents that you claim are 
//        infringed by the software, your patent license from such contributor to the software ends automatically. 
//    (C) If you distribute any portion of the software, you must retain all copyright, patent, 
//        trademark, and attribution notices that are present in the software. 
//    (D) If you distribute any portion of the software in source code form, you may do so 
//        only under this license by including a complete copy of this license with your distribution. 
//        If you distribute any portion of the software in compiled or object code form, you may only 
//        do so under a license that complies with this license. 
//    (E) The software is licensed "as-is." You bear the risk of using it. The contributors 
//        give no express warranties, guarantees, or conditions. You may have additional consumer 
//        rights under your local laws which this license cannot change. To the extent permitted 
//        under your local laws, the contributors exclude the implied warranties of merchantability, 
//        fitness for a particular purpose and non-infringement.

using System;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;
using log4net;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Smo.Agent;
using NDesk.Options;

namespace ScriptSqlConfig
{
    internal class Program
    {
        //NOTE: scripts out every database including tempdb
        //TODO: implement specific database scripting
        //TODO: implement bypass system databases and/or MS proprietary databases like reporting services
        #region setup private variables
        private static readonly ILog Log = LogManager.GetLogger(typeof(Program)); //where typeof() is the name of your class
        private static bool _scriptInstance = true;
        private static bool _scriptDatabases = true;
        private static bool _verbose;
        private static bool _jobsdrmode;
        private static string _server = "";
        private static string _directory = "";
        private static string _database = "";
        private static bool _showHelp;
        private static string _userName = "";
        private static string _password = "";
        #endregion

        private static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();

            var v = Assembly.GetExecutingAssembly().GetName().Version;

            var p = new OptionSet
                        {
                            {"v|verbose", z => _verbose = true},
                            {"s|server=", z => _server = z.Replace('/','\\')},//fix bad tick in name 
                            {"nodb", z => { _scriptDatabases = false; }},
                            {"drmode", z => { _jobsdrmode = true; }},
                            {"noinstance", z => { _scriptInstance = false; }},
                            {"d|dir|directory=", z => _directory = z},
                            {"database=", z => _database = z}, 
                            {"u|user=", z => _userName = z},
                            {"p|password=", z => _password = z},
                            {"h|?|help", z => { _showHelp = true; }}
                        };
            p.Parse(args);

            // the server and directory are required.  No args also brings out the help
            if (_server.Length == 0 || _directory.Length == 0 || args.Length == 0)
                _showHelp = true;

            // if they enter a username, require a password.
            if (_userName.Length > 0 && _password.Length == 0)
                _showHelp = true;

            #region Show Help

            if (_showHelp)
            {
                Console.WriteLine(@"
ScriptSqlConfig.EXE (" + v +
                                  @")

    This application generates scripts and configuration information
    for many SQL Server options and database objects.

    Required Parameters:
    ----------------------------------------------------------------

    /server ServerName
    /dir    OutputDirectory

    Optional Parameters:
    ----------------------------------------------------------------
    
    /v          (Verbose Output)
    /nodb       (Don't script databases)
    /database   (Specific database to script)
    /noinstance (Don't script instance information)
    /drmode     (Generate single file for jobs with preappended 
                 information and creates jobs disabled for DR
                 server)
    /user       (SQL Server user name.  It will use trusted
                 security unless this option is specified.)
    /password   (SQL Server password.  If /user is specified then
                 /password is required.)
    /?          (Display this help)

    Sample Usage
    ----------------------------------------------------------------

    ScriptSqlConfig.EXE /server Srv1\Instance /dir 'C:\MyDir'

    Notes
    ----------------------------------------------------------------
    1. If you have spaces in the path name, enclose it in quotes.
    2. It will use trusted authentication unless both the /username
       and /password are specified.
");

#if DEBUG
                Console.WriteLine("");
                Console.WriteLine("Press any key to continue....");
                Console.ReadLine();
#endif


                return;
            }

            #endregion

            Log.Info("Launching (" + v.Major + "." + v.Minor + "." + v.Build + ")....");
            Log.Info("Server: " + _server);
            Log.Info("Directory: " + _directory);


            if (_scriptInstance)
                ScriptInstance(_server, _directory);

            if (_scriptDatabases)
                ScriptAllDatabases(_server, _directory);

            Log.Info("Done.");

#if DEBUG
            Console.WriteLine("Press any key to continue....");
            Console.ReadLine();
#endif
        }

        private static void ScriptInstance(string server, string directory)
        {
            Log.Info("Scripting Instance information...");

            try
            {
                using (var conn = GetConnection(server, "master"))
                {
                    var instanceDirectory = directory;

                    try
                    {
                        //BUG: there is aparently a bug with SMO I am investigating now. only info I could find http://devio.wordpress.com/2010/12/20/strange-smo-errors/
                        WriteServerProperties(conn, instanceDirectory); 
                    }
                    catch (Exception e)
                    {
                        Log.ErrorFormat("Failed to script Server Properties.");
                        if (_verbose)
                            Log.Error(e);
                    }

                    try
                    {
                        ScriptLogins(conn, instanceDirectory);
                    }
                    catch (Exception e)
                    {
                        Log.ErrorFormat("Failed to script Logins.");
                        if (_verbose)
                            Log.Error(e);
                    }
                    
                    //            ScriptDatabaseMail(srv, instanceDirectory, so); //BUG: There's a bug that it doesn't script the SMTP server and port.
                    try
                    {
                        ScriptAgentJobs(conn, instanceDirectory);
                    }
                    catch (Exception e)
                    {
                        Log.ErrorFormat("Failed to script Agent Jobs.");
                        if (_verbose)
                            Log.Error(e);
                    }
                    try
                    {
                        ScriptLinkedServers(conn, instanceDirectory);
                    }
                    catch (Exception e)
                    {
                        Log.ErrorFormat("Failed to script Linked Servers.");
                        if (_verbose)
                            Log.Error(e);
                    }
                    

                }
            }
            catch (Exception e)
            {
                Log.ErrorFormat("Failed to script instances.");
                if (_verbose)
                    Log.Error(e);
                throw;
            }
        }

        private static SqlServerVersion GetTargetServerVersion(IServerInformation srv)
        {
            var version = srv.VersionMajor + srv.VersionMinor.ToString();
            if (version == "80")
                return SqlServerVersion.Version80;

            if (version == "90")
                return SqlServerVersion.Version90;

            if (version == "100")
                return SqlServerVersion.Version100;

            if (version == "1050")
                return SqlServerVersion.Version105;

            throw new Exception("Unsupported Server Version");
        }

        private static void WriteServerProperties(SqlConnection conn, string directory)
        {
            var srv = new Server(new ServerConnection(conn));
            var settings = new StringCollection();
            try
            {
                foreach (ConfigProperty p in srv.Configuration.Properties)
                {
                    try
                    {
                        settings.Add(p.DisplayName + " [" + p.RunValue + "]");
                    }
                    catch (Exception e)
                    {
                        Log.ErrorFormat("Failed to Script configuration property: {0}", p.RunValue);
                        if (_verbose)
                        {
                            Log.Error(e);
                            Console.ReadKey();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log.ErrorFormat("Failed to Script configuration properties");
                if (_verbose)
                {
                    Log.Error(e);
                    Console.ReadKey();
                }
            }

            try
            {
                WriteFile(settings, Path.Combine(directory, "sp_configure.txt"));
            }
            catch (Exception e)
            {
                Log.ErrorFormat("Failed to Script sp_configure.txt");
                if (_verbose)
                {
                    Log.Error(e);
                    Console.ReadKey();
                }
            }

            settings.Clear();
            try
            {

                foreach (Property x in srv.Properties)
                {
                    var propertyValue = x.Value ?? "NULL";
                    try
                    {
                        Log.Debug(x.Name + (" [" + propertyValue + "]") ?? "");
                        settings.Add(x.Name + (" [" + propertyValue + "]") ?? "");
                    }
                    catch (Exception e)
                    {
                        Log.ErrorFormat("Failed to Script property: {0}", propertyValue);
                        if (_verbose)
                        {
                            Log.Error(e);
                            Console.ReadKey();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                //BUG: ProcessorUsage in Properties collection throws an error sometimes claims missing stored proc 
                if (e.Message.Contains("An exception occurred while executing a Transact-SQL statement or batch."))
                {
                    Log.Error("Error Occured Please Check Properties.txt For Correctness");
                    Log.Debug(srv.Properties["SqlDomainGroup"].Name + (" [" + srv.Properties["SqlDomainGroup"].Value + "]") ?? "");
                    settings.Add(srv.Properties["SqlDomainGroup"].Name + (" [" + srv.Properties["SqlDomainGroup"].Value + "]") ?? "");
                }
                else
                {
                    Log.ErrorFormat("Failed to Script properties");
                    if (_verbose)
                    {
                        Log.Error(e);
                    }

                }
            }

            WriteFile(settings, Path.Combine(directory, "Properties.txt"));
        }

        private static void ScriptLogins(SqlConnection connection, string directory)
        {
            Log.Info("Scripting Logins...");
            var script = new StringCollection
                             {
                                 "----------------------------------------------------------------------",
                                 "-- Windows Logins and Groups",
                                 "----------------------------------------------------------------------",
                                 ""
                             };

            #region Windows Logins

            if (connection.State == ConnectionState.Closed)
                connection.Open();
            var cmd =
                new SqlCommand(
                    @"SELECT [name], [default_database_name]
FROM [sys].[server_principals]
where [type_desc] In ('WINDOWS_GROUP', 'WINDOWS_LOGIN')
AND [name] not like 'BUILTIN%'
and [NAME] not like 'NT AUTHORITY%'
and [name] not like '%\SQLServer%'
and [name] not like 'NT Service%'
ORDER BY [name]",
                    connection);


            using (var rdr = cmd.ExecuteReader())
            {
                while (rdr.Read())
                {
                    var createLogin =
                        @"IF NOT EXISTS (SELECT * FROM [master].[sys].[server_principals] WHERE [name] = '" +
                        rdr.GetString(rdr.GetOrdinal("name")) + @"')
	CREATE LOGIN [" +
                        rdr.GetString(rdr.GetOrdinal("name")) + @"] FROM WINDOWS 
        WITH DEFAULT_DATABASE=[" +
                        rdr.GetString(rdr.GetOrdinal("default_database_name")) + @"]
GO

";

                    script.Add(createLogin);
                }
                rdr.Close();
            }

            #endregion

            #region SQL Server Logins

            script.Add("----------------------------------------------------------------------");
            script.Add("-- SQL Server Logins");
            script.Add("----------------------------------------------------------------------");
            script.Add("");
            cmd =
                new SqlCommand(
                    @"SELECT [name], [sid] , [password_hash], [default_database_name], [is_expiration_checked], [is_policy_checked]
from [sys].[sql_logins] 
where type_desc = 'SQL_LOGIN' 
and [name] not in ('sa', 'guest')
and [name] not like '##%'
ORDER BY [name]",
                    connection);


            using (var rdr = cmd.ExecuteReader())
            {
                while (rdr.Read())
                {
                    var rawSid = new byte[85];
                    var length = rdr.GetBytes(rdr.GetOrdinal("sid"), 0, rawSid, 0, 85);
                    var sid = "0x" +
                                 BitConverter.ToString(rawSid).Replace("-", String.Empty).Substring(0, (int) length*2);

                    var rawPasswordHash = new byte[256];
                    length = rdr.GetBytes(rdr.GetOrdinal("password_hash"), 0, rawPasswordHash, 0, 256);
                    var passwordHash = "0x" +
                                          BitConverter.ToString(rawPasswordHash).Replace("-", String.Empty).Substring(
                                              0, (int) length*2);

                    var createLogin = @"IF NOT EXISTS (SELECT * FROM [master].[sys].[sql_logins] WHERE [name] = '" +
                                      rdr.GetString(rdr.GetOrdinal("name")) + @"')
	CREATE LOGIN [" +
                                      rdr.GetString(rdr.GetOrdinal("name")) + @"] 
		WITH PASSWORD = " +
                                      passwordHash + @" HASHED,
		SID = " + sid + @",  
		DEFAULT_DATABASE=[" +
                                      rdr.GetString(rdr.GetOrdinal("default_database_name")) +
                                      @"],  CHECK_POLICY=OFF, ";

                    if (rdr.GetBoolean(rdr.GetOrdinal("is_expiration_checked")))
                        createLogin += "CHECK_EXPIRATION = ON";
                    else
                        createLogin += "CHECK_EXPIRATION = OFF";


                    createLogin += @"
GO

";

                    createLogin += @"IF EXISTS (SELECT * FROM [master].[sys].[sql_logins] WHERE [name] = '" +
                                   rdr.GetString(rdr.GetOrdinal("name")) + @"')
	ALTER LOGIN [" +
                                   rdr.GetString(rdr.GetOrdinal("name")) + @"]
		WITH ";
                    if (rdr.GetBoolean(rdr.GetOrdinal("is_expiration_checked")))
                        createLogin += "CHECK_EXPIRATION = ON";
                    else
                        createLogin += "CHECK_EXPIRATION = OFF";

                    createLogin += ", ";

                    if (rdr.GetBoolean(rdr.GetOrdinal("is_policy_checked")))
                        createLogin += "CHECK_POLICY = ON";
                    else
                        createLogin += "CHECK_POLICY = OFF";


                    createLogin += @"
GO

";

                    script.Add(createLogin);
                }
                rdr.Close();
            }

            #endregion

            #region Disabled Logins

            cmd =
                new SqlCommand(
                    "SELECT [name] from [master].[sys].[server_principals] where [is_disabled] = 1 and [name] not like '##%'",
                    connection);
            using (SqlDataReader rdr = cmd.ExecuteReader())
            {
                if (rdr.HasRows)
                {
                    script.Add("----------------------------------------------------------------------");
                    script.Add("-- Disabled Logins");
                    script.Add("----------------------------------------------------------------------");
                    script.Add("");

                    while (rdr.Read())
                    {
                        script.Add("ALTER LOGIN [" + rdr.GetString(rdr.GetOrdinal("name")) + @"] DISABLE
GO

");
                    }
                }
                rdr.Close();
            }

            #endregion

            #region Group Membership

            cmd =
                new SqlCommand(
                    @"select l.[name] AS LoginName, r.name AS RoleName
from [master].[sys].[server_role_members] rm
join [master].[sys].[server_principals] r on r.[principal_id] = rm.[role_principal_id]
join [master].[sys].[server_principals] l on l.[principal_id] = rm.[member_principal_id]
where l.[name] not in ('sa')
AND l.[name] not like 'BUILTIN%'
and l.[NAME] not like 'NT AUTHORITY%'
and l.[name] not like '%\SQLServer%'
and l.[name] not like '##%'
and l.[name] not like 'NT Service%'
ORDER BY r.[name], l.[name]",
                    connection);

            using (SqlDataReader rdr = cmd.ExecuteReader())
            {
                if (rdr.HasRows)
                {
                    script.Add("----------------------------------------------------------------------");
                    script.Add("-- Role Membership");
                    script.Add("----------------------------------------------------------------------");
                    script.Add("");

                    while (rdr.Read())
                    {
                        script.Add("EXEC sp_addsrvrolemember @loginame = N'" +
                                   rdr.GetString(rdr.GetOrdinal("LoginName")) + @"', @rolename = N'" +
                                   rdr.GetString(rdr.GetOrdinal("RoleName")) + @"'
GO

");
                    }
                }
                rdr.Close();
            }

            #endregion

            WriteFile(script, Path.Combine(directory, "Logins.sql"));
        }

        /*
                //NOTE: commented out until fixed
                private static void ScriptDatabaseMail(Server smoServer, string directory)
                {
                    var so = new ScriptingOptions
                                 {
                                     ScriptDrops = false,
                                     IncludeIfNotExists = false,
                                     ClusteredIndexes = true,
                                     DriAll = true,
                                     Indexes = true,
                                     SchemaQualify = true,
                                     TargetServerVersion = GetTargetServerVersion(srv),
                                     Triggers = true,
                                     AnsiPadding = false
                                 };

                    WriteMessage("Scripting Database Mail...");
                    var script = smoServer.Mail.Script();
                    WriteFile(script, Path.Combine(directory, "DatabaseMail.sql"));
                }
        */

        private static void ScriptAgentJobs(SqlConnection conn, string directory)
        {
            //note: I know there is a clean up routine in the WriteFile method but it isn't catching all of it.
            var regexSearch = new string(Path.GetInvalidFileNameChars());
            var r = new Regex(string.Format("[{0}]", Regex.Escape(regexSearch)));

            var smoServer = new Server(new ServerConnection(conn));
            if (smoServer.EngineEdition == Edition.Express) return;
            Log.Info("Scripting Agent Jobs...");
            var jobDirectory = Path.Combine(directory, "Jobs");
            RemoveSqlFiles(jobDirectory);
            var jobs = new StringCollection();
            //var jobsEnabled = new StringCollection();
            var enableJobScript =
                "IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'Enable All Prodcution Jobs')" +
                Environment.NewLine +
                "EXEC msdb.dbo.sp_delete_job @job_name = N'Enable All Prodcution Jobs', @delete_unused_schedule=1" +
                Environment.NewLine +
                "GO" + Environment.NewLine +
                "EXEC msdb.dbo.sp_add_job @job_name = N'Enable All Prodcution Jobs'" + Environment.NewLine;
            var enableJobScriptStep =
                "EXEC msdb.dbo.sp_add_jobstep @job_name = N'Enable All Prodcution Jobs', @step_name = N'Enable Jobs', @subsystem = N'TSQL', @command = N'" +
                Environment.NewLine;
            Log.DebugFormat("Number of Jobs: {0}", smoServer.JobServer.Jobs.Count);
            foreach (Job job in smoServer.JobServer.Jobs)
            {
                var script = job.Script();
                string dropJob;
                if (job.IsEnabled)
                {
                    dropJob =
                        "IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'" + _server +
                        " - DR - Enabled - " + job.Name + "')" +
                        Environment.NewLine +
                        "EXEC msdb.dbo.sp_delete_job @job_name = N'" + _server +
                        " - DR - Enabled - " + job.Name +
                        "', @delete_unused_schedule=1" +
                        Environment.NewLine +
                        "GO";
                }
                else
                {
                    dropJob =
                        "IF  EXISTS (SELECT job_id FROM msdb.dbo.sysjobs_view WHERE name = N'" + _server +
                        " - DR - Disabled - " + job.Name + "')" +
                        Environment.NewLine +
                        "EXEC msdb.dbo.sp_delete_job @job_name = N'" + _server +
                        " - DR - Disabled - "  + job.Name +
                        "', @delete_unused_schedule=1" +
                        Environment.NewLine +
                        "GO";
                }

                jobs.Add(dropJob);

                if (_jobsdrmode)
                {
                    var i = 0;
                    while (i < script.Count)
                    {
                        if (script[i].Contains("@job_name"))
                        {
                            if (job.IsEnabled)
                            {
                                script[i] = script[i].Replace("@job_name=N'", "@job_name=N'" + _server + " - DR - Enabled - ");
                                script[i] = script[i].Replace("@enabled=1", "@enabled=0");
                                enableJobScriptStep += "exec msdb.dbo.sp_update_job @job_name = ''" + _server + " - DR - Enabled - " +
                                                       job.Name +
                                                       "'',@enabled = 1" + Environment.NewLine;
                            }
                            else
                            {
                                script[i] = script[i].Replace("@job_name=N'", "@job_name=N'" + _server + " - DR - Disabled - ");
                            }
                        }
                        jobs.Add(script[i] + Environment.NewLine + "GO" + Environment.NewLine);
                        i++;
                    }
                }
                else
                {
                    //single job per file
                    WriteFile(script, Path.Combine(jobDirectory, r.Replace(job.Name + "_jobs.sql", "_")));
                }
            }
            if (!_jobsdrmode) return;
            enableJobScriptStep += "'" + Environment.NewLine;
            jobs.Add(enableJobScript);
            jobs.Add(enableJobScriptStep);

            //write one file with all jobs
            WriteFile(jobs, Path.Combine(jobDirectory, r.Replace(_server + "_jobs.sql", "_")));

            //NOTE: commented out until fixed
            //script = smoServer.Mail.Script();
        }

        private static void ScriptLinkedServers(SqlConnection conn, string directory)
        {
            var smoServer = new Server(new ServerConnection(conn));
            Log.Info("Scripting Linked Servers...");
            var linkedServerDirectory = Path.Combine(directory, "Linked Servers");
            RemoveSqlFiles(linkedServerDirectory);
            foreach (LinkedServer linkedServer in smoServer.LinkedServers)
            {
                var script = linkedServer.Script();
                var serverName = linkedServer.Name.Replace(@"\", "_");
                WriteFile(script, Path.Combine(linkedServerDirectory, serverName + ".sql"), true);
            }
        }

        private static void ScriptAllDatabases(string server, string directory)
        {
            Log.Info("Scripting databases...");

            var databasesDirectory = Path.Combine(directory, "Databases");
            RemoveSqlFiles(databasesDirectory);
            try
            {
                using (var conn = GetConnection(server, "master"))
                {
                    var srv = new Server(new ServerConnection(conn));
                    var singleDB = new Database();
                    if (!string.IsNullOrEmpty(_database))
                    {
                        //if (db.IsAccessible)
                        //{
                        //    Log.Info("Scripting Database: " + db.Name);
                        //    var outputDirectory = Path.Combine(databasesDirectory, db.Name);
                        //    try
                        //    {
                        //        ScriptDatabase(server, db.Name, outputDirectory);
                        //    }
                        //    catch (Exception e)
                        //    {
                        //        Log.ErrorFormat("Failed to Script Database: {0}", db.Name);
                        //        if (_verbose)
                        //            Log.Error(e);
                        //    }
                        //}
                        //else
                        //{
                        //    Log.InfoFormat("Skipping Database:{0}", db.Name);
                        //}
                    }
                    else
                    {
                        foreach (Database db in srv.Databases)
                        {
                            if (db.IsAccessible)
                            {
                                Log.Info("Scripting Database: " + db.Name);
                                var outputDirectory = Path.Combine(databasesDirectory, db.Name);
                                try
                                {
                                    ScriptDatabase(server, db.Name, outputDirectory);
                                }
                                catch (Exception e)
                                {
                                    Log.ErrorFormat("Failed to Script Database: {0}", db.Name);
                                    if (_verbose)
                                        Log.Error(e);
                                }
                            }
                            else
                            {
                                Log.InfoFormat("Skipping Database:{0}", db.Name);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error("Failed to Script Databases.");
                if (_verbose)
                    Log.Error(e);
            }
        }

        private static void ScriptDatabase(string server, string database, string directory)
        {
            var so = new ScriptingOptions
                         {
                             ScriptDrops = false,
                             IncludeIfNotExists = false,
                             ClusteredIndexes = true,
                             DriAll = true,
                             Indexes = true,
                             SchemaQualify = true,
                             Triggers = true,
                             AnsiPadding = false
                         };

            var conn = GetConnection(server, database);
            var srv = new Server(new ServerConnection(conn));

            so.TargetServerVersion = GetTargetServerVersion(srv);
            var db = srv.Databases[database];

            srv.SetDefaultInitFields(typeof (StoredProcedure), "IsSystemObject");

            #region Tables

            var objectDir = Path.Combine(directory, "Tables");
            RemoveSqlFiles(objectDir);
            foreach (Table t in db.Tables)
            {
                if (t.IsSystemObject) continue;
                string fileName = null;
                StringCollection sc = null;

                if (_verbose)
                    Log.Info("Table: " + t.Name);
                try
                {
                    sc = t.Script(so);
                }
                catch (Exception e)
                {
                    Log.ErrorFormat("Failed To Script Table: {0}",t.Name);
                    if (_verbose)
                        Log.Error(e);
                }

                try
                {
                    fileName = Path.Combine(objectDir, t.Schema + "." + t.Name + ".sql");
                }
                catch (Exception e)
                {
                    Log.ErrorFormat("Failed To Script Table: {0}", t.Name);
                    if (_verbose)
                        Log.Error(e);
                }
                WriteFile(sc, fileName, true);
            }

            #endregion

            #region Stored Procedures

            objectDir = Path.Combine(directory, "Sprocs");
            RemoveSqlFiles(objectDir);
            foreach (StoredProcedure sp in db.StoredProcedures)
            {
                string fileName = null;
                StringCollection sc = null;
                if (sp.IsSystemObject) continue;
                if (_verbose)
                    Log.Info("Sproc: " + sp.Name);
                try
                {
                    sc = sp.Script(so);
                }
                catch (Exception e)
                {
                    Log.ErrorFormat("Failed To Script Procedure: {0}", sp.Name);
                    if (_verbose)
                        Log.Error(e);
                }

                try
                {
                    fileName = Path.Combine(objectDir, sp.Schema + "." + sp.Name + ".sql");
                }
                catch (Exception e)
                {
                    Log.ErrorFormat("Failed To Script Procedure: {0}", sp.Name);
                    if (_verbose)
                        Log.Error(e);
                }

                WriteFile(sc, fileName, true);
            }

            #endregion

            #region User-defined data types

            objectDir = Path.Combine(directory, "DataTypes");
            RemoveSqlFiles(objectDir);
            foreach (UserDefinedDataType udt in db.UserDefinedDataTypes)
            {
                string fileName = null;
                StringCollection sc = null;

                if (_verbose)
                    Log.Info("DataType: " + udt.Name);
                try
                {
                    sc = udt.Script(so);
                }
                catch (Exception e)
                {
                    Log.ErrorFormat("Failed To Script User Definded Function: {0}", udt.Name);
                    if (_verbose)
                        Log.Error(e);
                }

                try
                {
                    Path.Combine(objectDir, udt.Schema + "." + udt.Name + ".sql");
                }
                catch (Exception e)
                {
                    Log.ErrorFormat("Failed To Script User Definded Function: {0}", udt.Name);
                    if (_verbose)
                        Log.Error(e);
                }
                WriteFile(sc, fileName, true);
            }

            #endregion

            #region Views

            objectDir = Path.Combine(directory, "Views");
            RemoveSqlFiles(objectDir);
            foreach (View v in db.Views)
            {
                if (v.IsSystemObject) continue;
                string fileName = null;
                StringCollection sc = null;

                if (_verbose)
                    Log.Info("View: " + v.Name);
                try
                {
                    sc = v.Script(so);
                }
                catch (Exception e)
                {
                    Log.ErrorFormat("Failed To Script View: {0}", v.Name);
                    if (_verbose)
                        Log.Error(e);
                }

                try
                {
                    fileName = Path.Combine(objectDir, v.Schema + "." + v.Name + ".sql");
                }
                catch (Exception e)
                {
                    Log.ErrorFormat("Failed To Script View: {0}", v.Name);
                    if (_verbose)
                        Log.Error(e);
                }
                WriteFile(sc, fileName, true);
            }

            #endregion

            #region Triggers

            objectDir = Path.Combine(directory, "DDLTriggers");
            RemoveSqlFiles(objectDir);
            foreach (DatabaseDdlTrigger tr in db.Triggers)
            {
                string fileName = null;
                StringCollection sc = null;

                if (tr.IsSystemObject) continue;
                if (_verbose)
                    Log.Info("DDL Trigger: " + tr.Name);
                try
                {
                    sc = tr.Script(so);
                }
                catch (Exception e)
                {
                    Log.ErrorFormat("Failed To Script DDL Trigger: {0}", tr.Name);
                    if (_verbose)
                        Log.Error(e);

                }

                try
                {
                    fileName = Path.Combine(objectDir, tr.Name + ".sql");
                }
                catch (Exception e)
                {
                    Log.ErrorFormat("Failed To Script DDL Trigger: {0}", tr.Name);
                    if (_verbose)
                        Log.Error(e);

                }
                WriteFile(sc, fileName, true);
            }

            #endregion

            #region Table Types

            if (srv.VersionMajor >= 10)
            {
                objectDir = Path.Combine(directory, "TableTypes");
                RemoveSqlFiles(objectDir);
                foreach (UserDefinedTableType tt in db.UserDefinedTableTypes)
                {

                    string fileName = null;
                    StringCollection sc = null;
                    if (_verbose)
                        Log.Info("TableType: " + tt.Name);
                    try
                    {
                        sc = tt.Script(so);
                    }
                    catch (Exception e)
                    {
                        Log.ErrorFormat("Failed to script TableType: {0}",tt.Name);
                        if (_verbose)
                            Log.Error(e);

                    }

                    try
                    {
                        fileName = Path.Combine(objectDir, tt.Schema + "." + tt.Name + ".sql");
                    }
                    catch (Exception e)
                    {
                        Log.ErrorFormat("Failed To Script Table Type: {0}", tt.Name);
                        if (_verbose)
                            Log.Error(e);
                    }

                    WriteFile(sc, fileName, true);
                }
            }

            #endregion

            #region Assemblies

            //objectDir = Path.Combine(directory, "Assemblies");
            //RemoveSqlFiles(objectDir);
            //foreach (SqlAssembly asm in db.Assemblies)
            //{
            //    if (!asm.IsSystemObject)
            //    {
            //        WriteMessage("Assembly: " + asm.Name);
            //        StringCollection sc = asm.Script(so);
            //        string fileName = Path.Combine(objectDir,asm.Name + ".sql");

            //        WriteFile(sc, fileName);
            //    }
            //}

            #endregion

            #region User-Defined Functions

            objectDir = Path.Combine(directory, "UDF");
            RemoveSqlFiles(objectDir);
            foreach (UserDefinedFunction udf in db.UserDefinedFunctions)
            {
                if (udf.IsSystemObject) continue;
                string fileName = null;
                StringCollection sc = null;
                if (_verbose)
                    Log.Info("UDF: " + udf.Name);
                try
                {
                    sc = udf.Script(so);
                }
                catch (Exception e)
                {
                    Log.ErrorFormat("Failed To Script User Defined Funtion: {0}", udf.Name);
                    if (_verbose)
                        Log.Error(e);
                }

                try
                {
                    fileName = Path.Combine(objectDir, udf.Schema + "." + udf.Name + ".sql");
                }
                catch (Exception e)
                {
                    Log.ErrorFormat("Failed To Script User Defined Funtion: {0}", udf.Name);
                    if (_verbose)
                        Log.Error(e);
                }
                WriteFile(sc, fileName, true);
            }

            #endregion
        }

        private static void RemoveSqlFiles(string directory)
        {
            var dir = new DirectoryInfo(directory);
            if (dir.Exists)
            foreach (var f in dir.GetFiles("*.sql"))
                try
                {
                    f.Delete();
                }
                catch (Exception e)
                {
                    Log.ErrorFormat("Failed to Delte File: {0}",f.Name);
                    Log.Error(e);
                }
        }

        private static void WriteFile(StringCollection script, string fileName, bool addGoStatements = false)
        {
            if (script == null) return;
            if (fileName == null) return;

            // Clean up an invalid characters
            // pull apart the file passed in and remove any funky characters
            var directory = Path.GetDirectoryName(fileName);
            var fileOnly = Path.GetFileName(fileName);


            var regexSearch = new string(Path.GetInvalidFileNameChars());
            var r = new Regex(string.Format("[{0}]", Regex.Escape(regexSearch)));
            if (fileOnly != null) fileOnly = r.Replace(fileOnly, "");

            if (directory != null) if (fileOnly != null) fileName = Path.Combine(directory, fileOnly);

            if (File.Exists(fileName))
                File.Delete(fileName);


            if (directory != null)
                if (!Directory.Exists(directory))
                    Directory.CreateDirectory(directory);

            TextWriter tw = new StreamWriter(fileName);
            foreach (var s in script)
            {
                var stringToWrite = s;
                if (!stringToWrite.EndsWith(Environment.NewLine))
                    stringToWrite += Environment.NewLine;

                tw.Write(stringToWrite);
                //if (s.StartsWith("SET QUOTED_IDENTIFIER") || s.StartsWith("SET ANSI_NULLS"))
                //    tw.Write(System.Environment.NewLine + "GO");

                if (!addGoStatements) continue;
                // if the previous line doesn't endwith a NewLine, write one
                if (!stringToWrite.EndsWith(Environment.NewLine))
                    tw.Write(Environment.NewLine);

                tw.WriteLine("GO" + Environment.NewLine);
            }

            tw.Close();
            return;
        }

        private static SqlConnection GetConnection(string serverName, string databaseName)
        {
            var csb = new SqlConnectionStringBuilder {DataSource = serverName};


            if (_userName.Length > 0)
            {
                csb.IntegratedSecurity = false;
                csb.UserID = _userName;
                csb.Password = _password;
            }
            else
            {
                csb.IntegratedSecurity = true;
            }


            csb.InitialCatalog = databaseName;
            csb.ApplicationName = "ScriptSqlConfig";
            var c = new SqlConnection(csb.ConnectionString);
            return c;
        }
    }
}