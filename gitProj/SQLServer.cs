using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace SqlOptimizeTask
{
    class SqlServer : IDisposable
    {
        // These are the standard set of trace flags used in automated testing.  They control
        // creating crash dumps on failures and dismissing assert dialogs.  For more detail
        // on traceflags see sql\ntdbms\ntinc\traceflg.h in sql sources.
        //
        static readonly string[] automatedTestTraceFlags = new string[]
        {
            "-T6527", //Disable generation of dump on 1st CLR OOM/Stack overflow exception 
            "-T2539", //Use external debugger after internal dump
            "-T4042", //Do not pop up dialog if assertion happens
            "-T2558", //Disable DBCC CHECKDB Watson integration
            "-T2590", //Generate full dump file along with minidump.
            "-T2591", //Do an infinite wait for the sqldumper process to return
            "-T4602", //Disable the password policy enforcement.
            "-T2592"  //do the symbolic dump in the process
        };

        const string SqlServerStartCmd = @"sqlservr.cmd";
        bool isStopped;

        string sqlServerBinn;
        string sqlServerRoot;

        Process serverProcess;

        public SqlServer(string serverRoot)
        {
            Logger.WriteLine("SqlServer serverRoot={0}", serverRoot);
            sqlServerRoot = serverRoot;
            sqlServerBinn = Path.Combine(sqlServerRoot, "binn");
            isStopped = true;
        }

        // rollbacks start off as no-ops.  rollbacks set when registry is modified.
        //
        Action RollbackNativeClientOdbc = () => { };
        Action RollbackNativeClientOdbc64 = () => { };
        Action RollbackNativeClientOledb = () => { };

        /// <summary>
        /// Starts server.
        /// </summary>
        public void Start(string[] traceflags)
        {
            Logger.WriteLine("Starting server.");
            string sqlServerCmdPath = Path.Combine(this.sqlServerRoot, SqlServerStartCmd);
            string runSqlServerCmd = Path.Combine(this.sqlServerRoot, "runsqlserver.cmd");
            using (StreamWriter writer = new StreamWriter(runSqlServerCmd))
            {
                writer.WriteLine(@"cd ""{0}""", this.sqlServerRoot);
                writer.WriteLine("{0} -noregistration {1} > sqlserver.out", sqlServerCmdPath, string.Join(" ", traceflags));
            }

            Logger.WriteLine("start server script:\r\n{0}", File.ReadAllText(runSqlServerCmd));

            serverProcess = Process.Start("cmd.exe", "/C " + runSqlServerCmd);
            WaitForServerRecovery();

            isStopped = false;
        }

        void WaitForServerRecovery()
        {
            string wait4SqlExe = Path.Combine(this.sqlServerRoot, "wait4sql.exe");

            // Waiting for 2 minutes for server startup to complete. Prior to 3/2018 this was 1 minute,
            // but sometimes the timeout would be reached and fail a build.
            //
            int waitMilliseconds = 120*1000;
            string arguments = string.Format("{0}", waitMilliseconds);
            
            Stopwatch timer = Stopwatch.StartNew();
            //
            // Start wait4sql and wait a little bit longer than the timeout for it to complete.
            //
            Logger.WriteLine("WaitForServerRecovery command line {0} {1}", wait4SqlExe, arguments);
            Process p = Process.Start(wait4SqlExe, arguments);
            p.WaitForExit(waitMilliseconds + 3000);
            timer.Stop();

            if(p.ExitCode != 0)
            {
                throw new SqlServerException(string.Format("Timed out waiting for server recovery after {0} milliseconds", timer.ElapsedMilliseconds));
            }

            Logger.WriteLine("Sql server fully recovered after {0} milliseconds.", timer.ElapsedMilliseconds);
        }


        public void Stop()
        {
            Logger.WriteLine("Stopping server.");

            // If the server is running attempt to stop it with the T-SQL shutdown command.
            //
            if (isStopped == false)
            {
                try
                {
                    SqlScript shutdownServer = new SqlScript("shutdown");

                    // Waiting for 2 minutes for server shutdown (similar to
                    // startup) to complete.. Prior to 8/2019 this was 15s
                    //
                    shutdownServer.Execute("localhost", TimeSpan.FromMinutes(2));

                    // Start the next step after a small delay
                    //
                    Thread.Sleep(TimeSpan.FromMinutes(2));
                }
                catch (Exception)
                {
                    Logger.WriteLine("Server was already stopped.");
                }

                isStopped = true;
            }

            if (serverProcess != null && !serverProcess.HasExited)
            {
                serverProcess.Kill();
            }
        }

        // This config comes from sqlnclidrv11.rgs in sqlservr directory.  This allows meta to get to the 
        // sql native client dll built with product.  It would be great if we could have ODBC find the
        // driver though a mechanism other than the registry.
        RegistryRoot CreateNativeClientOdbcConfig(string sqlncliPath)
        {
            return new RegistryRoot(RegistryHive.LocalMachine,
                new RegistryKeyEntry("SOFTWARE",
                    new RegistryKeyEntry("ODBC",
                        new RegistryKeyEntry("ODBCINST.INI", new[] {
                            new RegistryKeyEntry("SQL Server Native Client 11.0", new [] {
                                new RegistryValueEntry("UsageCount", 1),
                                new RegistryValueEntry("Driver", sqlncliPath),
                                new RegistryValueEntry("Setup", sqlncliPath),
                                new RegistryValueEntry("APILevel", "2"),
                                new RegistryValueEntry("ConnectFunctions", "YYY"),
                                new RegistryValueEntry("CPTimeout", "60"),
                                new RegistryValueEntry("DriverODBCVer", "03.80"),
                                new RegistryValueEntry("FileUsage", "0"),
                                new RegistryValueEntry("SQLLevel", "1"),
                            }),
                            new RegistryKeyEntry("ODBC Drivers",
                                new RegistryValueEntry("SQL Server Native Client 11.0", "Installed"))
                        })
                    )
                )
            );
        }

        // This config comes from sqlnclidrv.rgs in sqlservr directory.  This allows meta to get to the 
        // sql native client dll built with product.  It would be great if we could have ODBC find the
        // driver though a mechanism other than the registry.
        RegistryRoot CreateNativeClientOdbcConfig64(string sqlncliPath)
        {
            return new RegistryRoot(RegistryHive.LocalMachine, RegistryView.Registry64,
                new RegistryKeyEntry("SOFTWARE",
                    new RegistryKeyEntry("ODBC",
                        new RegistryKeyEntry("ODBCINST.INI", new[] {
                            new RegistryKeyEntry("ODBC Driver 11 for SQL Server", new [] {
                                new RegistryValueEntry("UsageCount", 1),
                                new RegistryValueEntry("Driver", "msodbcsql17.dll"),
                                new RegistryValueEntry("Setup", "msodbcsql17.dll"),
                                new RegistryValueEntry("APILevel", "2"),
                                new RegistryValueEntry("ConnectFunctions", "YYY"),
                                new RegistryValueEntry("CPTimeout", "60"),
                                new RegistryValueEntry("DriverODBCVer", "03.80"),
                                new RegistryValueEntry("FileUsage", "0"),
                                new RegistryValueEntry("SQLLevel", "1"),
                            }),
                            new RegistryKeyEntry("ODBC Drivers",
                                new RegistryValueEntry("ODBC Driver 11 for SQL Server", "Installed"))
                        })
                    )
                )
            );
        }

        // this config comes from sqlncli.rgs in sqlservr directory.  This allows the server to execute distributed queries using
        // sql native client OLEDB.  It would be great if we could replace this with a mechanism that does not rely on
        // registry.
        RegistryRoot CreateNativeClientOledbConfig(string sqlncliPath)
        {
            return new RegistryRoot(RegistryHive.ClassesRoot, new[] {
            //return new RegistryRoot(RegistryHive.CurrentUser, new RegistryKeyEntry("Software", new RegistryKeyEntry("Classes", new [] {
                new RegistryKeyEntry("SQLNCLI11.Enumerator", "SQL Server Native Client 11.0 Enumerator", 
                    new RegistryKeyEntry("Clsid", "{8F612DD2-7E28-424f-A2FD-C2EECC314AA2}")),
                new RegistryKeyEntry("SQLNCLI11.Enumerator.1", "SQL Server Native Client 11.0 Enumerator", 
                    new RegistryKeyEntry("Clsid", "{8F612DD2-7E28-424f-A2FD-C2EECC314AA2}")),
                
                new RegistryKeyEntry("SQLNCLI11.ErrorLookup", "SQL Server Native Client 11.0 Error Lookup", 
                    new RegistryKeyEntry("Clsid", "{CA99D701-E6E7-4db4-A5CC-81541C75188A}")),
                new RegistryKeyEntry("SQLNCLI11.ErrorLookup.1", "SQL Server Native Client 11.0 Error Lookup", 
                    new RegistryKeyEntry("Clsid", "{CA99D701-E6E7-4db4-A5CC-81541C75188A}")),

                new RegistryKeyEntry("SQLNCLI11", "SQL Server Native Client 11.0", 
                    new RegistryKeyEntry("Clsid", "{397C2819-8272-4532-AD3A-FB5E43BEAA39}")),
                new RegistryKeyEntry("SQLNCLI11.1", "SQL Server Native Client 11.0", 
                    new RegistryKeyEntry("Clsid", "{397C2819-8272-4532-AD3A-FB5E43BEAA39}")),
                
                new RegistryKeyEntry("SQLNCLI11.ConnectionPage", "SQL Server Native Client 11.0 Connection Page", 
                    new RegistryKeyEntry("Clsid", "{8F8A2489-F677-43bf-B991-0BBE263147C8}")),
                new RegistryKeyEntry("SQLNCLI11.ConnectionPage.1", "SQL Server Native Client 11.0 Connection Page", 
                    new RegistryKeyEntry("Clsid", "{8F8A2489-F677-43bf-B991-0BBE263147C8}")),

                new RegistryKeyEntry("SQLNCLI11.AdvancedPage", "SQL Server Native Client 11.0 Advanced Page", 
                    new RegistryKeyEntry("Clsid", "{D2E5582D-7771-4777-89A2-90C374777FDB}")),
                new RegistryKeyEntry("SQLNCLI11.AdvancedPage.1", "SQL Server Native Client 11.0 Advanced Page", 
                    new RegistryKeyEntry("Clsid", "{D2E5582D-7771-4777-89A2-90C374777FDB}")),

                new RegistryKeyEntry("CLSID", new []{
                    new RegistryKeyEntry("{397C2819-8272-4532-AD3A-FB5E43BEAA39}", "SQLNCLI11", new RegistryEntry[] {
                        new RegistryValueEntry("OLEDB_SERVICES", 4294967295),
                        new RegistryKeyEntry("ProgID", "SQLNCLI11.1"),
                        new RegistryKeyEntry("VersionIndependentProgID", "SQLNCLI11"),
                        new RegistryKeyEntry("InprocServer32", sqlncliPath, 
                            new RegistryValueEntry("ThreadingModel", "Both")),
                        new RegistryKeyEntry("OLE DB Provider", "SQL Server Native Client 11.0"),
                        new RegistryKeyEntry("ExtendedErrors", "Extended Error Service",
                            new RegistryKeyEntry("{CA99D701-E6E7-4db4-A5CC-81541C75188A}", "SQLNCLI11 Error Lookup")),
                        new RegistryKeyEntry("Implemented Categories", 
                            new RegistryKeyEntry("{D267E19A-0B97-11D2-BB1C-00C04FC9B532}"))
                    }),
                    new RegistryKeyEntry("{8F612DD2-7E28-424f-A2FD-C2EECC314AA2}", "SQLNCLI11 Enumerator", new RegistryEntry[] {
                        new RegistryKeyEntry("ProgID", "SQLNCLI11.Enumerator.1"),
                        new RegistryKeyEntry("VersionIndependentProgID", "SQLNCLI11.Enumerator"),
                        new RegistryKeyEntry("InprocServer32", sqlncliPath, 
                            new RegistryValueEntry("ThreadingModel", "Both")),
                        new RegistryKeyEntry("OLE DB Enumerator", "SQL Server Native Client 11.0 Enumerator")
                    }),
                    new RegistryKeyEntry("{CA99D701-E6E7-4db4-A5CC-81541C75188A}", "SQLNCLI11 Error Lookup", new RegistryEntry[] {
                        new RegistryKeyEntry("ProgID", "SQLNCLI11.ErrorLookup.1"),
                        new RegistryKeyEntry("VersionIndependentProgID", "SQLNCLI11.ErrorLookup"),
                        new RegistryKeyEntry("InprocServer32", sqlncliPath, 
                            new RegistryValueEntry("ThreadingModel", "Both")),
                    }),
                    new RegistryKeyEntry("{8F8A2489-F677-43bf-B991-0BBE263147C8}", "SQLNCLI11 Connection Page", new RegistryEntry[] {
                        new RegistryKeyEntry("ProgID", "SQLNCLI11.ConnectionPage.1"),
                        new RegistryKeyEntry("VersionIndependentProgID", "SQLNCLI11.ConnectionPage"),
                        new RegistryKeyEntry("InprocServer32", sqlncliPath, 
                            new RegistryValueEntry("ThreadingModel", "Both")),
                    }),
                    new RegistryKeyEntry("{D2E5582D-7771-4777-89A2-90C374777FDB}", "SQLNCLI11 Advanced Page", new RegistryEntry[] {
                        new RegistryKeyEntry("ProgID", "SQLNCLI11.AdvancedPage.1"),
                        new RegistryKeyEntry("VersionIndependentProgID", "SQLNCLI11.AdvancedPage"),
                        new RegistryKeyEntry("InprocServer32", sqlncliPath, 
                            new RegistryValueEntry("ThreadingModel", "Both")),
                    }),
                }),
                
            });

        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void Dispose(bool disposing)
        {
            if(disposing)
            {
                try
                {
                    RollbackNativeClientOdbc();
                }
                catch (Exception exception)
                {
                    Logger.WriteLine(string.Format("Couldn't rollback msodbcsql17 ODBC registry configuration. {0}", exception));
                }
                try
                {
                    RollbackNativeClientOdbc64();
                }
                catch (Exception exception)
                {
                    Logger.WriteLine(string.Format("Couldn't rollback x64 msodbcsql17 ODBC registry configuration. {0}", exception));
                }
                try
                {
                    RollbackNativeClientOledb();
                }
                catch (Exception exception)
                {
                    Logger.WriteLine(string.Format("Couldn't rollback sqlncli11 OLEDB registry configuration. {0}", exception));
                }
                Stop();
            }
        }
    }
}
