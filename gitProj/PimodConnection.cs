using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Security;
using System.Threading;
using Microsoft.SqlServer.Test.Utilities;
using System.Text;
using System.Text.RegularExpressions;

namespace Microsoft.SqlServer.Test.Pimod
{
	/// <summary>
	/// Provides the interface for managing connections to the SQL server and executing T-SQL batches.
	/// </summary>
	/// <example>
	/// Create a <see cref="PimodConnection" /> using the <see cref="PimodConnectionInfo" />
	/// <code lang="C#">
	/// PimodConnectionInfoBuilder pcib = new PimodConnectionInfoBuilder(TestServerName);
	/// pcib.UseIntegratedSecurity = false;
	/// pcib.UserName = userName;
	/// pcib.Password = password;
	/// return CreatePimodConnection(pcib.ToPimodConnectionInfo());
	/// 
	/// // Create a command and then execute it
	/// PimodDataReader reader = _pimodConnection.ExecuteReader(command);
	/// </code>
	/// </example>
	[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1506:AvoidExcessiveClassCoupling", Justification = "This is a complicated class with a lot of dependencies")]
	public abstract class PimodConnection : IDisposable
	{
		#region Fields
		private bool _disposed;
		private bool? _isCloud;
		private string _originalDatabaseName;
		private PimodConnectionInfo _pimodConnectionInfo;
		private int _commandRetryCount;
		private ServerConnectionProperties _serverConnectionProperties;
		private Session _session;
		private short _spid;
		private SqlInstance _sqlInstance;
		private ThreadWithExceptionHandler _asyncThread;
		private PimodConnectionPerformanceContainer _performanceContainer;
		private static long _lastUniqueId;
		private readonly long _uniqueId;
		private static readonly PimodCommandInfo _resetConnectionCommandInfo = new PimodCommandInfo("sp_reset_connection", CommandType.StoredProcedure);
		private static ConcurrentDictionary<string, bool> _providerInstalledDictionary = new ConcurrentDictionary<string, bool>();

		#endregion

		#region Constructor and Create

		/// <summary>
		/// Initializes a new instance of the <see cref="PimodConnection"/> class.
		/// </summary>
		/// <param name="pimodConnectionInfo">The pimod connection info.</param>
		protected PimodConnection(PimodConnectionInfo pimodConnectionInfo)
		{
			_pimodConnectionInfo = pimodConnectionInfo;
			_uniqueId = Interlocked.Increment(ref _lastUniqueId);
		}

		/// <summary>
		/// <para>Factory method for creating a PimodConnection.  The <see cref="PimodConnectionCreated"/> event will be raised, which allows executor such as TestShell to log the activity.</para>
		/// <para>Typically, when you need a PimodConnection within a TestShell test, you should use TargetSqlEnvironment.CreatePimodConnection() to avoid the need to pass extra information </para>
		/// <para>but the events will still be raised if you call this method directly.</para>
		/// </summary>
		/// <param name="pimodConnectionInfo"><see cref="PimodConnectionInfo"/> for the connection.</param>
		public static PimodConnection Create(PimodConnectionInfo pimodConnectionInfo)
		{
			return Create(pimodConnectionInfo, false);
		}

		/// <summary>
		///   <para>Factory method for creating a PimodConnection.  If <paramref name="suppressCreationEvent" /> is false, the <see cref="PimodConnectionCreated" /> event will be raised, which allows executor such as TestShell to log the activity.</para>
		///   <para>Typically, when you need a PimodConnection within a TestShell test, you should use TargetSqlEnvironment.CreatePimodConnection() to avoid the need to pass extra information </para>
		///    <para>but the events will still be raised if you call this method directly.</para>
		/// </summary>
		/// <param name="pimodConnectionInfo"><see cref="PimodConnectionInfo" /> for the connection.</param>
		/// <param name="suppressCreationEvent">Suppresses the creation event.  This can be used to hide the connection within internal Pimod code or to prevent TestShell from logging the connection</param>
		/// <param name="openConnection">if set to <c>true</c> the connection will be opened before returned.</param>
		/// <returns></returns>
		public static PimodConnection Create(PimodConnectionInfo pimodConnectionInfo, bool suppressCreationEvent, bool openConnection = true)
		{
			return Create(pimodConnectionInfo, suppressCreationEvent, false, openConnection);
		}

		internal static PimodConnection Create(PimodConnectionInfo pimodConnectionInfo, bool suppressCreationEvent, bool initializeSpid, bool openConnection)
		{
			if (!openConnection && initializeSpid)
			{
				throw new ArgumentException("Cannot initialize spid without opening the connection.");
			}

			// Update the password at authentication time if specified
			//
			if (pimodConnectionInfo.NewPassword != null)
			{
				//
				// Change the password during authentication
				//
				SqlConnection.ChangePassword(PimodSqlConnection.CreateSqlClientConnectionString(pimodConnectionInfo), pimodConnectionInfo.NewPassword);

				// Update the pimodConnectionInfo with the new password so it can be properly used
				//
				PimodConnectionInfoBuilder pimodConnectionInfoBuilder = new PimodConnectionInfoBuilder(pimodConnectionInfo);
				pimodConnectionInfoBuilder.Password = pimodConnectionInfo.NewPassword;
				pimodConnectionInfoBuilder.NewPassword = null;
				pimodConnectionInfo = pimodConnectionInfoBuilder.ToPimodConnectionInfo();
			}

			// Raise the creating event as long as we haven't been asked to suppress it
			//
			if (!suppressCreationEvent)
			{
				OnPimodConnectionCreating(pimodConnectionInfo);
			}

			// Abstract out constructor for future proof reasons -- We could add a OleDb/SNAC implementation of PimodConnection if desired...
			//
			PimodConnection connection = null;
			Stopwatch timeToCreate = Stopwatch.StartNew();
			switch (pimodConnectionInfo.ConnectionType)
			{
				case PimodConnectionType.SqlClient:
					connection = new PimodSqlConnection(pimodConnectionInfo);
					break;
				case PimodConnectionType.Odbc:
					connection = new PimodOdbcConnection(pimodConnectionInfo);
					break;
				case PimodConnectionType.OleDb:
					connection = new PimodOleDbConnection(pimodConnectionInfo);
					break;
				default:
					throw new ArgumentException("Unknown connection type: " + pimodConnectionInfo.ConnectionType);
			}

			if (openConnection)
			{
				OnPimodConnectionOpeningDuringCreate(connection);
				connection.Open();
			}

			try
			{
				timeToCreate.Stop();

				// Initialize the SPID if asked.  Do this before firing the event so that it's not logged.
				//
				if (initializeSpid)
				{
					connection.InitializeSpid();
				}

				// Cache the original database name in order to use it for reset
				//
				connection.CacheDatabaseName();

				// Enable info message event
				//
				connection.ToggleInfoMessageEventing(true);

				// Fire the created event and return the created connection
				//
				if (!suppressCreationEvent)
				{
					OnPimodConnectionCreated(connection, timeToCreate.Elapsed);
				}
			}
			catch (Exception)
			{
				connection.Dispose();
				throw;
			}

			return connection;
		}

		#endregion

		#region Properties

		private static readonly string[] AllowedInvalidOperationExceptionMessages = new[]
		{
			"Operation cancelled by user.",
			"Invalid operation. The connection is closed.",
			"requires an open and available Connection. The connection's current state is closed."
		};

		/// <summary>
		/// Gets the command retry count. Valid within
		/// the scope of a command execution.
		/// </summary>
		/// <value>
		/// The command retry count.
		/// </value>
		public int CommandRetryCount
		{
			get { return _commandRetryCount; }
		}

		/// <summary>
		/// Gets the generic db connection.
		/// </summary>
		/// <value>The db connection.</value>
		protected abstract IDbConnection DbConnection
		{
			get;
		}

		/// <summary>
		/// Gets the generic db transaction.
		/// </summary>
		/// <value>The db transaction.</value>
		protected internal abstract IDbTransaction DbTransaction
		{
			get;
		}

		/// <summary>
		/// Returns true if the current PimodConnection is currently executing an asynchronous command via BeginExecute.
		/// </summary>
		public bool IsExecutingAsynchronously
		{
			get { return _asyncThread != null; }
		}

		/// <summary>
		/// Gets the <see cref="PimodConnectionPerformanceContainer"/> which contains methods specific to testing query performance
		/// </summary>
		public PimodConnectionPerformanceContainer Performance
		{
			get
			{
				if (_performanceContainer == null)
				{
					_performanceContainer = new PimodConnectionPerformanceContainer(this);
				}
				return _performanceContainer;
			}
		}


		/// <summary>
		///  Gets the connection info used to create the connection
		/// </summary>
		public PimodConnectionInfo PimodConnectionInfo { get { return _pimodConnectionInfo; } }

		/// <summary>
		///  Gets the SQL session for the connection
		/// </summary>
		public Session Session
		{
			get
			{
				if (_session == null)
				{
					_session = new Session(this);
				}
				return _session;
			}
		}

		/// <summary>
		/// Gets the SPID
		/// </summary>
		/// <remarks>
		/// The first access will cause the SPID queried and cached for subsequent access
		/// </remarks>
		internal short Spid
		{
			get
			{
				if (_spid == 0)
				{
					throw new InvalidOperationException("SPID must be initialized within the constructor to be accessed");
				}

				return _spid;
			}
		}

		/// <summary>
		/// Gets the state of the Connection
		/// </summary>
		/// <value>The state.</value>
		public ConnectionState State
		{
			get { return DbConnection.State; }
		}


		/// <summary>
		/// Returns server-level information about the connections to SQL Server (pulled from sys.dm_exec_connections) 
		/// </summary>
		public ServerConnectionProperties ServerConnectionProperties
		{
			get
			{
				if (_serverConnectionProperties == null)
				{
					_serverConnectionProperties = new ServerConnectionProperties(this);
				}
				return _serverConnectionProperties;
			}
		}

		/// <summary>
		/// Returns true if the provider supports beginning named transactions.
		/// </summary>
		/// <remarks>This is true for SqlClient, but false for other providers.</remarks>
		public virtual bool SupportsNamedTransactions
		{
			get { return false; }
		}

		/// <summary>
		/// Returns true if the provider supports save transactions
		/// </summary>
		/// <remarks>This is true for SqlClient, but false for other providers.</remarks>
		public virtual bool SupportsSaveTransactions
		{
			get { return false; }
		}

		/// <summary>
		/// Gets the state of the connection
		/// </summary>
		/// <value>The current database name.</value>
		public string DatabaseName { get { return DbConnection.Database; } }

		/// <summary>
		/// Gets/sets the associated sql instance.  It is necessary to have an associated sql instance when querying UDTs since this allows the <see cref="PimodUdtType"/> to be properly resolved. 
		/// </summary>
		public SqlInstance AssociatedSqlInstance
		{
			get { return _sqlInstance; }
			set { _sqlInstance = value; }
		}

		/// <summary>
		/// Gets the unique id of this PimodConnection. This is assigned at the client side and is not visible to the server.
		/// </summary>
		public long UniqueId
		{
			get { return _uniqueId; }
		}

		/// <summary>
		/// Gets the string representation for the MSOLEDBSQL provider name.
		/// </summary>
		public static string MSOLEDBSQLProviderName
		{
			get { return "MSOLEDBSQL"; }
		}

		/// <summary>
		/// Gets the string representation for the SQLNCLI11 provider name.
		/// </summary>
		public static string SQLNCLI11ProviderName
		{
			get { return "SQLNCLI11"; }
		}
		#endregion

		#region Events
		/// <summary>
		/// Occurs when the ExecuteNonQuery completes
		/// </summary>
		public event EventHandler<NonQueryExecutedEventArgs> NonQueryExecuted;

		/// <summary>
		/// Occurs when ExecuteReader completes
		/// </summary>
		public event EventHandler<DataReaderExecutedEventArgs> DataReaderExecuted;

		/// <summary>
		/// Occurs when the execution of a command starts.  This event can be used to transform the command.  For the purpose of logging, use the <see cref="CommandNowExecuting"/> event instead.
		/// </summary>
		public event EventHandler<CommandExecutingEventArgs> CommandExecuting;

		/// <summary>
		/// Occurs when the database is changing.
		/// </summary>
		public event EventHandler<DatabaseChangingEventArgs> DatabaseChanging;

		/// <summary>
		/// Occurs when a transaction is about to begin.
		/// </summary>
		public event EventHandler<TransactionBeginningEventArgs> TransactionBeginning;

		/// <summary>
		/// Occurs when a transaction is about to commit.
		/// </summary>
		public event EventHandler<TransactionCommittingEventArgs> TransactionCommitting;

		/// <summary>
		/// Occurs when a transaction is about to roll back.
		/// </summary>
		public event EventHandler<TransactionRollingBackEventArgs> TransactionRollingBack;

		/// <summary>
		/// Occurs when a transaction is about to be saved.
		/// </summary>
		public event EventHandler<TransactionSavingEventArgs> TransactionSaving;

		/// <summary>
		/// Static event that is fired whenever a <see cref="PimodConnection"/> is created.
		/// </summary>
		/// <remarks>Since this is a static event, the sender will be null.  To grab the created <see cref="PimodConnection"/>, use the <see cref="PimodConnectionCreatedEventArgs"/>.</remarks>
		public static event EventHandler<PimodConnectionCreatedEventArgs> PimodConnectionCreated;

		/// <summary>
		/// Static event that is fired just before a <see cref="PimodConnection"/> is created.
		/// </summary>
		/// <remarks>Since this is a static event, the sender will be null.  To access the <see cref="PimodConnectionInfo"/> , use the <see cref="PimodConnectionCreatingEventArgs"/>.</remarks>
		public static event EventHandler<PimodConnectionCreatingEventArgs> PimodConnectionCreating;

		/// <summary>
		/// Static event that is fired just before the <see cref="PimodConnection"/> is opened while inside Create. This will happen only if the openConnection boolean is set to true (which is the case by default) during Create.
		/// </summary>
		public static event EventHandler<PimodConnectionOpeningDuringCreateEventArgs> PimodConnectionOpeningDuringCreate;

		/// <summary>
		/// Occurs when an information message is received from the connection.  
		/// </summary>
		/// <remarks>
		/// This event occurs when the server returns a message with a severity of 10 or less.  To listen for messages with larger severity, subscribe to the <see cref="PimodSqlExceptionReceived"/> event.
		/// 
		/// Test code should not be subscribing to this event.  Instead, use the <see cref="NonQueryResult.InfoMessages"/> or <see cref="PimodDataReader.GetInfoMessages"/> APIs.
		/// </remarks>
		public event EventHandler<PimodSqlErrorsReceivedEventArgs> PimodSqlErrorsReceived;

		/// <summary>
		/// Occurs when the server throws an error of severity 11 of greater.  
		/// </summary>
		/// <remarks>
		/// Test code should not be subscribing to this event.  Instead, catch the <see cref="PimodSqlException"/> thrown.
		/// </remarks>
		public event EventHandler<PimodSqlExceptionReceivedEventArgs> PimodSqlExceptionReceived;

		/// <summary>
		/// Occurs immediately before the execution of a command.  This event should be used for logging purposes.  The command should not be altered within this event.  If you wish to alter the command, please use the <see cref="CommandExecuting"/> event instead.
		/// </summary>
		public event EventHandler<CommandExecutingEventArgs> CommandNowExecuting;

		/// <summary>
		/// Occurs after a retryable exception is thrown during a command execution.
		/// </summary>
		public event EventHandler<RetryOccurredEventArgs> CommandRetryOccurred;

		/// <summary>
		/// Occurs immediately after a command execution failed.
		/// </summary>
		public event EventHandler<CommandFailedEventArgs> CommandFailed;

		/// <summary>
		/// Occurs when retryable command has finished (successfully or not).
		/// </summary>
		public event EventHandler<RetryableActionEndedEventArgs> RetryableActionEnded;

		/// <summary>
		/// Occurs after a retryable exception is thrown when opening a connection.
		/// </summary>
		public event EventHandler<RetryOccurredEventArgs> ConnectionRetryOccurred;

		/// <summary>
		/// Occurs when the connection is being reopened.
		/// </summary>
		public event EventHandler ConnectionReopening;

		/// <summary>
		/// Occurs when Pimod attempts to log in to the remote server.
		/// </summary>
		public event EventHandler ConnectionOpening;

		/// <summary>
		/// Occurs when login is completed. It either succeeded or failed without an exception.
		/// </summary>
		/// <remarks>
		/// This event is static since it happens during construction.
		/// </remarks>
		public event EventHandler<ConnectionOpenedEventArgs> ConnectionOpened;

		/// <summary>
		/// Occurs when login failed with an exception.
		/// </summary>
		/// <remarks>
		/// This event is static since it happens during construction.
		/// </remarks>
		public event EventHandler<ConnectionOpenFailedEventArgs> ConnectionOpenFailed;

		#endregion

		#region Transaction Related Methods

		#region Asynchronous

		/// <summary>
		/// Commits the transaction asynchronously on the current session.
		/// You must call EndExecute to complete the asynchronous execution.
		/// </summary>
		/// <param name="postExceptionCallback">The post exception callback. Default is null.</param>
		/// <remarks>
		/// There should be an active transaction for this operation to succeed
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is not an active transaction</exception>
		public void BeginCommitTransaction(Action<Exception> postExceptionCallback = null)
		{
			if (!HasActiveTransaction)
			{
				InvalidOperationException invalidOperationException = new InvalidOperationException("There is not an active transaction to commit");
				if (postExceptionCallback != null)
				{
					postExceptionCallback(invalidOperationException);
				}
				throw invalidOperationException;
			}

			ThrowIfAsyncExists();
			_asyncThread = new ThreadWithExceptionHandler(
				CommitTransaction,
				e =>
				{
					if (postExceptionCallback != null)
					{
						postExceptionCallback(e);
					}
				});
			_asyncThread.Start();
		}

		/// <summary>
		/// Saves the transaction asynchronously
		/// You must call EndExecute to complete the asynchronous execution.
		/// </summary>
		/// <param name="savePointName">Name of the save point.</param>
		/// <param name="postExceptionCallback">The post exception callback. Default is null.</param>
		/// <remarks>
		/// There should be an active transaction for this operation to succeed
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is not an active transaction</exception>
		public void BeginSaveTransaction(string savePointName, Action<Exception> postExceptionCallback = null)
		{
			if (!HasActiveTransaction)
			{
				InvalidOperationException invalidOperationException = new InvalidOperationException("There is not an active transaction to save");
				if (postExceptionCallback != null)
				{
					postExceptionCallback(invalidOperationException);
				}
				throw invalidOperationException;
			}

			ThrowIfAsyncExists();
			_asyncThread = new ThreadWithExceptionHandler(
				() => SaveTransaction(savePointName),
				e =>
				{
					if (postExceptionCallback != null)
					{
						postExceptionCallback(e);
					}
				});
			_asyncThread.Start();
		}

		/// <summary>
		/// Rolls back a transaction asynchronously
		/// You must call EndExecute to complete the asynchronous execution.
		/// </summary>
		/// <remarks>
		/// There should be an active transaction for this operation to succeed
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is not an active transaction</exception>
		public void BeginRollbackTransaction()
		{
			BeginRollbackTransaction(null);
		}

		/// <summary>
		/// Rolls back a transaction asynchronously
		/// You must call EndExecute to complete the asynchronous execution.
		/// </summary>
		/// <param name="transactionName">The transaction or savepoint name</param>
		/// <param name="postExceptionCallback">The post exception callback. Default is null.</param>
		/// <remarks>
		/// There should be an active transaction for this operation to succeed
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is not an active transaction</exception>
		public void BeginRollbackTransaction(string transactionName, Action<Exception> postExceptionCallback = null)
		{
			if (!HasActiveTransaction)
			{
				InvalidOperationException invalidOperationException = new InvalidOperationException("There is not an active transaction to rollback");
				if (postExceptionCallback != null)
				{
					postExceptionCallback(invalidOperationException);
				}
				throw invalidOperationException;
			}

			ThrowIfAsyncExists();
			_asyncThread = new ThreadWithExceptionHandler(
				() => RollbackTransaction(transactionName),
				e =>
				{
					if (postExceptionCallback != null)
					{
						postExceptionCallback(e);
					}
				});
			_asyncThread.Start();
		}

		#endregion

		/// <summary>
		/// Begins the transaction with Unspecified IsolationLevel.
		/// </summary>
		/// <remarks>
		/// There should be only one active transaction per session.
		/// This method will throw an exception if there is already an active transaction
		/// in this session.
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is already an active transaction</exception>
		public void BeginTransaction()
		{
			BeginTransaction(IsolationLevel.Unspecified);
		}

		/// <summary>
		/// Starts a transaction on the current session.
		/// </summary>
		/// <param name="isolationLevel">Isolation level</param>
		/// <remarks>
		/// There should be only one active transaction per session.
		/// This method will throw an exception if there is already an active transaction
		/// in this session.
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is already an active transaction</exception>
		public void BeginTransaction(IsolationLevel isolationLevel)
		{
			BeginTransaction(isolationLevel, null);
		}

		/// <summary>
		/// Starts a transaction on the current session.
		/// </summary>
		/// <param name="isolationLevel">Isolation level</param>
		/// <param name="transactionName"></param>
		/// <remarks>
		/// There should be only one active transaction per session.
		/// This method will throw an exception if there is already an active transaction
		/// in this session.
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is already an active transaction</exception>
		/// <exception cref="InvalidOperationException">Occurs when there is a non-null <paramref name="transactionName"/> and <see cref="SupportsNamedTransactions"/> is false</exception>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Done to allow for connection abstraction")]
		public void BeginTransaction(IsolationLevel isolationLevel, string transactionName)
		{
			if (HasActiveTransaction)
			{
				throw new InvalidOperationException("Nested transactions are not suppported");
			}
			if (!SupportsNamedTransactions && transactionName != null)
			{
				throw new ArgumentException("Named transaction are not supported by this provider " + PimodConnectionInfo.ConnectionType);
			}

			OnTransactionBeginning(isolationLevel, transactionName);
			try
			{
				BeginTransactionCore(isolationLevel, transactionName);
			}
			catch (Exception e)
			{
				OnExceptionReceived(e);
			}
		}

		/// <summary>
		/// Starts a transaction on the current session.
		/// </summary>
		/// <param name="isolationLevel">The isolation level.</param>
		/// <param name="transactionName">Name of the transaction.</param>
		/// <remarks>
		/// There should be only one active transaction per session.
		/// This method will throw an exception if there is already an active transaction
		/// in this session.
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is already an active transaction</exception>
		/// <exception cref="InvalidOperationException">Occurs when there is a non-null <paramref name="transactionName"/> and <see cref="SupportsNamedTransactions"/> is false</exception>
		public void BeginTransaction(TransactionIsolationLevel isolationLevel, string transactionName = null)
		{
			BeginTransaction(ConvertIsolationLevel(isolationLevel), transactionName);
		}

		/// <summary>
		/// Starts a transaction on the current session.
		/// </summary>
		/// <param name="isolationLevel">Isolation level</param>
		/// <param name="transactionName"></param>
		/// <remarks>
		/// There should be only one active transaction per session.
		/// This method will throw an exception if there is already an active transaction
		/// in this session.
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is already an active transaction</exception>
		/// <exception cref="ArgumentException">Occurs if IsolationLevel.Unspecified is passed</exception>
		protected abstract void BeginTransactionCore(IsolationLevel isolationLevel, string transactionName);

		/// <summary>
		/// Commits the transaction on the current session.
		/// </summary>
		/// <remarks>
		/// There should be an active transaction for this operation to succeed
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is not an active transaction</exception>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Done to allow for connection abstraction")]
		public void CommitTransaction()
		{
			if (!HasActiveTransaction)
			{
				throw new InvalidOperationException("There is not an active transaction to commit");
			}

			// It is not possible to have multiple transactions within SqlClient, so there is not a Commit() overload with the transaction name
			//
			OnTransactionCommitting();

			try
			{
				DbTransaction.Commit();
			}
			catch (Exception e)
			{
				OnExceptionReceived(e);
			}
			ResetTransactionState();
		}

		/// <summary>
		/// Saves the transaction
		/// </summary>
		/// <param name="savePointName">Name of the save point.</param>
		/// <remarks>
		/// There should be an active transaction for this operation to succeed
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is not an active transaction</exception>
		/// <exception cref="InvalidOperationException">Occurs when <see cref="SupportsSaveTransactions"/> is false</exception>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Done to allow for connection abstraction")]
		public void SaveTransaction(string savePointName)
		{
			if (!SupportsSaveTransactions)
			{
				throw new InvalidOperationException("The connection provider does not support save points: " + PimodConnectionInfo.ConnectionType);
			}

			if (!HasActiveTransaction)
			{
				throw new InvalidOperationException("There is not an active transaction to save");
			}

			OnTransactionSaving(savePointName);

			try
			{
				SaveTransactionInternal(savePointName);
			}
			catch (Exception e)
			{
				OnExceptionReceived(e);
			}
		}

		/// <summary>
		/// Saves the transaction
		/// </summary>
		/// <param name="savePointName">Name of the save point.</param>
		/// <remarks>
		/// There should be an active transaction for this operation to succeed
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is not an active transaction</exception>
		protected virtual void SaveTransactionInternal(string savePointName)
		{
			throw new NotImplementedException("BUG IN PIMOD: This method should not have been called");
		}

		/// <summary>
		/// Rolls back a transaction
		/// </summary>
		/// <remarks>
		/// There should be an active transaction for this operation to succeed
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is not an active transaction</exception>
		public void RollbackTransaction()
		{
			RollbackTransaction(null);
		}

		/// <summary>
		/// Rolls back a transaction
		/// </summary>
		/// <param name="transactionName">The transaction or savepoint name</param>
		/// <remarks>
		/// There should be an active transaction for this operation to succeed
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is not an active transaction</exception>
		/// <exception cref="InvalidOperationException">Occurs when there is a non-null <paramref name="transactionName"/> and <see cref="SupportsNamedTransactions"/> is false</exception>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Done to allow for connection abstraction")]
		public void RollbackTransaction(string transactionName)
		{
			if (!HasActiveTransaction)
			{
				throw new InvalidOperationException("There is not an active transaction to rollback");
			}
			if (!SupportsNamedTransactions && transactionName != null)
			{
				throw new ArgumentException("Named transaction are not supported by this provider " + PimodConnectionInfo.ConnectionType);
			}

			OnTransactionRollingBack(transactionName);

			try
			{
				RollbackTransactionInternal(transactionName);
			}
			catch (Exception e)
			{
				OnExceptionReceived(e);
			}
			CheckForZombieTransaction();
		}

		/// <summary>
		/// Rolls back a transaction
		/// </summary>
		/// <param name="transactionName">The transaction or savepoint name</param>
		/// <remarks>
		/// There should be an active transaction for this operation to succeed
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is not an active transaction</exception>
		protected abstract void RollbackTransactionInternal(string transactionName);

		/// <summary>
		/// Returns whether or not a transaction is currently active
		/// </summary>
		/// <value>
		/// 	<c>true</c> if this instance has active transaction; otherwise, <c>false</c>.
		/// </value>
		/// <remarks>
		/// This will become false after RollbackTransaction() or CommitTransaction() or
		/// if the server rolls back the transaction due to XACT_ABORT settings / severity levels
		/// </remarks>
		public bool HasActiveTransaction
		{
			get { return DbTransaction != null; }
		}

		/// <summary>
		/// Resets the state of the transaction.
		/// </summary>
		protected abstract void ResetTransactionState();

		/// <summary>
		/// Checks for zombie transaction.
		/// </summary>
		protected internal void CheckForZombieTransaction()
		{
			if (DbTransaction != null && DbTransaction.Connection == null)
			{
				// SqlTransaction has a feature where Connection is reset to null if the transaction has been rolled back on the server,
				// thus making the SqlTransaction "Zombied"
				// 
				ResetTransactionState();

				// TODO: If we add Transaction events, raise the RolledbackTransaction event here and include that it was server induced
				//
			}
		}
		#endregion

		#region Misc Methods

		/// <summary>
		/// Cache the original database name so that Reset can return the connection to the original database.
		/// </summary>
		private void CacheDatabaseName()
		{
			_originalDatabaseName = this.DatabaseName;
		}

		/// <summary>
		/// Tries to cancel the command currently being executed.
		/// </summary>
		/// <remarks>
		/// If there is nothing to cancel, nothing occurs.  However, if there is a command in process, and the attempt to cancel fails, no exception is generated. 
		/// If MARS is enabled, a cancel will be attempted on all active commands.
		/// </remarks>
		public abstract void Cancel();

		/// <summary>
		/// Determines whether a connection or command retry is configured.
		/// </summary>
		private bool IsConnectionOrCommandRetryConfigured()
		{
			return _pimodConnectionInfo.CommandRetryPolicyInfo != null || _pimodConnectionInfo.ConnectionRetryPolicyInfo != null;
		}

		/// <summary>
		/// Changes the database to the supplied database name
		/// </summary>
		/// <param name="databaseName">Database name</param>
		public void ChangeDatabase(string databaseName)
		{
			if (databaseName == null)
			{
				throw new ArgumentNullException("databaseName");
			}

			OnDatabaseChanging(this.DatabaseName, databaseName);

			if (ShouldRetryCommand())
			{
				ExecuteWithRetry(_pimodConnectionInfo.CommandRetryPolicyInfo, () => { ChangeDatabaseInternal(databaseName); }, OnCommandRetrying, OnRetryableActionEnded);
			}
			else
			{
				ChangeDatabaseInternal(databaseName);
			}
		}

		/// <summary>
		/// Method for underlying PimodConnection implementation to change the database.
		/// </summary>
		/// <param name="databaseName">Name of the database.</param>
		protected abstract void ChangeDatabaseInternal(string databaseName);

		/// <summary>
		/// Changes the database if it is different from the database the connection is currently using.
		/// </summary>
		/// <param name="databaseName">The database name to change to.</param>
		private void ChangeDatabaseIfDifferentFromCurrentDatabase(string databaseName)
		{
			if (databaseName != null && !databaseName.Equals(DatabaseName, StringComparison.OrdinalIgnoreCase))
			{
				ChangeDatabase(databaseName);
			}
		}

		/// <summary>
		/// Provides an enumerator for all installed OLEDB Provider names.
		/// </summary>
		private static IEnumerable<string> EnumOleDbProviderNames()
		{
			// https://docs.microsoft.com/en-us/dotnet/api/system.data.oledb.oledbenumerator.getrootenumerator?view=netframework-4.7.2
			//
			const int SOURCES_NAME = 0;

			using (OleDbDataReader dr = OleDbEnumerator.GetRootEnumerator())
			{
				while (dr.HasRows && dr.Read())
				{
					yield return (string)dr[SOURCES_NAME];
				}
			}
		}

		/// <summary>
		/// Checks if an OLE DB provider has been installed and registered. Requires connection information
		/// to test construction of the provider.
		/// </summary>
		/// <param name="providerName">The name of the provider.</param>
		/// <param name="testConnectionInfo">The connection info used to create the test connection.</param>
		/// <returns>
		/// True if the provider has been installed and registered.
		/// </returns>
		public static bool CheckIfOleDbProviderIsInstalled(string providerName, PimodConnectionInfo testConnectionInfo)
		{
			if (string.IsNullOrEmpty(providerName))
			{
				throw new ArgumentException("providerName cannot be null or empty");
			}

			bool providerInstalled = false;
			string providerNameUpper = providerName.ToUpper();

			// We cache the result of the provider check since it requires a connection test
			// to verify that the provider is installed.
			//
			if (_providerInstalledDictionary.ContainsKey(providerNameUpper))
			{
				providerInstalled = _providerInstalledDictionary[providerNameUpper];
			}
			else
			{
				int count = EnumOleDbProviderNames()
						.Where(name => name.ToUpper() == providerNameUpper).Count();

				if (count > 0)
				{
					// Test the connection since it is possible for the provider to be
					// in the enumerator when it is not installed properly.
					//
					PimodConnectionInfoBuilder builder = new PimodConnectionInfoBuilder(testConnectionInfo);
					builder.NativeDriver = providerName;
					builder.ConnectionType = PimodConnectionType.OleDb;
					builder.ConnectionTimeout = 30;
					
					// Note: we can safely call this since CreateOleDbConnectionString() will not recurse and call
					// CheckIfOleDbProviderIsInstalled() again as long as NativeDriver is provided.
					//
					string connectionString = PimodOleDbConnection.CreateOleDbConnectionString(builder.ToPimodConnectionInfo());

					try
					{
						using (OleDbConnection conn = new OleDbConnection(connectionString))
						{
							// Calling Open() will force the creation of the provider instance.
							// If this succeeds, then the provider is installed.
							//
							conn.Open();
							providerInstalled = true;
						}
					}
					catch
					{
						// If we hit an exception, assume that the provider is not available
						//
						providerInstalled = false;
					}
					
					_providerInstalledDictionary.TryAdd(providerNameUpper, providerInstalled);
				}
			}

			return providerInstalled;
		}

		private static IsolationLevel ConvertIsolationLevel(TransactionIsolationLevel isolationLevel)
		{
			switch (isolationLevel)
			{
				case TransactionIsolationLevel.ReadCommitted:
					return IsolationLevel.ReadCommitted;
				case TransactionIsolationLevel.ReadUncomitted:
					return IsolationLevel.ReadUncommitted;
				case TransactionIsolationLevel.RepeatableRead:
					return IsolationLevel.RepeatableRead;
				case TransactionIsolationLevel.Serializable:
					return IsolationLevel.Serializable;
				case TransactionIsolationLevel.Snapshot:
					return IsolationLevel.Snapshot;
				case TransactionIsolationLevel.Unspecified:
					return IsolationLevel.Unspecified;
				default:
					throw new InvalidOperationException(string.Format("Unknown TransactionIsolationLevel: {0}.", isolationLevel));
			}
		}

		/// <summary>
		/// Repetitively executes the specified function while it satisfies the current retry policy.
		/// </summary>
		/// <param name="retryPolicyInfo">The <see cref="RetryPolicyInfo" /> that determines retry parameters.</param>
		/// <param name="nonQueryAction">A delegate representing the executable action.</param>
		/// <param name="onRetryEvent">The event handler that's called if a retry occurs.</param>
		/// <param name="onRetryableActionEndedEvent">The event handler to call when retryable action has ended.</param>
		private void ExecuteWithRetry(RetryPolicyInfo retryPolicyInfo, Action nonQueryAction, EventHandler<RetryOccurredEventArgs> onRetryEvent, EventHandler<RetryableActionEndedEventArgs> onRetryableActionEndedEvent)
		{
			// So we can reuse the same code in ExecuteAction<T> we create a wrapper Func that always returns true.
			//
			ExecuteWithRetry(retryPolicyInfo, () => { nonQueryAction(); return true; }, null, onRetryEvent, onRetryableActionEndedEvent);
		}

		/// <summary>
		/// Repetitively executes the specified function while it satisfies the current retry policy.
		/// </summary>
		/// <typeparam name="T">The type of result expected from the executable action.</typeparam>
		/// <param name="retryStrategy">The <see cref="IRetryStrategy" /> that determines retry parameters.</param>
		/// <param name="functionToExecute">A delegate representing the executable action which returns the result of type <typeparamref name="T" />.</param>
		/// <param name="getExceptionFromResult">An optional delegate that allows the caller to specify how the exception is extracted from the result of type <typeparamref name="T" />. If null passed the exceptions thrown by the functionToExecute parameter are used.</param>
		/// <param name="onRetryEvent">The event handler that's called if a retry occurs.</param>
		/// <param name="onRetryableActionEndedEvent">The retryable action ended event.</param>
		/// <returns>
		/// The result from the action.
		/// </returns>
		private T ExecuteWithRetry<T>(IRetryStrategy retryStrategy, Func<T> functionToExecute, Func<T, Exception> getExceptionFromResult, EventHandler<RetryOccurredEventArgs> onRetryEvent, EventHandler<RetryableActionEndedEventArgs> onRetryableActionEndedEvent = null)
		{
			int currentRetryCount = 0;
			Stopwatch timeSpent = Stopwatch.StartNew();
			bool isOpen = State == ConnectionState.Open;

			while (true)
			{
				Exception lastException = null;
				_commandRetryCount = currentRetryCount;

				RetryInfo retryInfo;
				try
				{
					T result = functionToExecute();

					if (getExceptionFromResult != null)
					{
						// The caller has specified a custom method for extracting exceptions from the result.
						//
						lastException = getExceptionFromResult(result);

						retryInfo = retryStrategy.GetRetryInfo(lastException, currentRetryCount, timeSpent.Elapsed);
						if (lastException == null || !retryInfo.ShouldRetry)
						{
							if (onRetryableActionEndedEvent != null)
							{
								onRetryableActionEndedEvent(this, new RetryableActionEndedEventArgs(currentRetryCount, isSuccessful: lastException == null));
							}
							return result;
						}
					}
					else
					{
						if (onRetryableActionEndedEvent != null)
						{
							onRetryableActionEndedEvent(this, new RetryableActionEndedEventArgs(currentRetryCount, isSuccessful: true));
						}
						return result;
					}
				}
				catch (Exception ex)
				{
					lastException = ex;

					retryInfo = retryStrategy.GetRetryInfo(lastException, currentRetryCount, timeSpent.Elapsed);
					InvalidOperationException invalidOperationException = ex as InvalidOperationException;
					if (invalidOperationException == null ||
						!AllowedInvalidOperationExceptionMessages.Any(m => invalidOperationException.Message.Contains(m)))
					{
						if (!retryInfo.ShouldRetry)
						{
							if (onRetryableActionEndedEvent != null)
							{
								onRetryableActionEndedEvent(this, new RetryableActionEndedEventArgs(currentRetryCount, isSuccessful: false));
							}
							throw;
						}
					}
				}

				++currentRetryCount;
				TimeSpan delay = retryInfo.RetryInterval.HasValue ? retryInfo.RetryInterval.Value : TimeSpan.FromSeconds(_pimodConnectionInfo.DefaultCommandTimeout);

				// Perform an extra safety check in the delay interval. Should prevent from accidentally 
				// ending up with the value of -1 which will block a thread indefinitely. 
				// In addition, any other negative numbers will cause an ArgumentOutOfRangeException
				// fault which will be thrown by Thread.Sleep.
				//
				if (delay.TotalMilliseconds < 0)
				{
					delay = TimeSpan.Zero;
				}

				onRetryEvent(this, new RetryOccurredEventArgs(currentRetryCount, lastException, delay, isOpen));

				Thread.Sleep(delay);
			}
		}

		/// <summary>
		/// Determines if retrying a command is possible based on the state of the connection and 
		/// the CommandRetryPolicy.
		/// </summary>
		private bool ShouldRetryCommand()
		{
			// Inside a transaction, we have no idea what other commands have already been executed
			// so we can't support retrying on a failure, expected or not. There are also no retries
			// if the command retry policy is not configured.
			//
			return !HasActiveTransaction && _pimodConnectionInfo.CommandRetryPolicyInfo != null;
		}

		/// <summary>
		/// Check if the connection is connecting to Cloud environment.
		/// </summary>
		private bool IsCloud
		{
			get
			{
				if (!_isCloud.HasValue)
				{
					if (_sqlInstance != null)
					{
						_isCloud = (_sqlInstance.Edition == SqlEdition.Azure);
					}
					else
					{
						_isCloud = (int)ExecuteScalar("SELECT CASE WHEN SERVERPROPERTY('Edition') = 'SQL Azure' THEN 1 ELSE 0 END") == 1;
					}
				}
				return _isCloud.Value;
			}
		}

		internal void InitializeSpid()
		{
			_spid = (short)Session.SessionId;
		}

		/// <summary>
		/// Reopens the connection if closed. This is used by the retry logic in the case of transient errors. 
		/// <remarks>
		/// If the Open fails it may trigger a connection retrying event.
		/// </remarks>
		/// </summary>
		private void ReopenConnectionIfClosed()
		{
			if (State != ConnectionState.Open)
			{
				OnConnectionReopening();
				Open();
			}
		}

		/// <summary>
		/// Force a connection reset in the same way clients manage connection pooling reuse
		/// </summary>
		public void ResetConnection()
		{
			ResetConnectionInternal();

			// Invoking sp_reset_connection returns the connection to the original database, however it does not return an env_change to the client
			// so the client does not know the database has changed. In order to avoid this, we explicitly change the database back to the original database
			//
			if (DatabaseName != _originalDatabaseName)
			{
				ChangeDatabase(_originalDatabaseName);
			}

			_session = null;
			if (HasActiveTransaction)
			{
				ResetTransactionState();
			}
		}

		/// <summary>
		/// Resets the connection by executing the appropriate command on the connection, overridden for specific implementations.
		/// </summary>
		protected virtual void ResetConnectionInternal()
		{
			// this is the default implementation
			//
			ExecuteNonQueryInternal(_resetConnectionCommandInfo, true);
		}

		/// <summary>
		/// Opens the connection using the <see cref="RetryPolicyInfo"/> for connections and <see cref="ImpersonationWrapper"/> if set.
		/// </summary>
		public void Open(bool doNotRetry = false)
		{
			using (ImpersonationWrapper impersonationContext = _pimodConnectionInfo.CreateImpersonationWrapperIfSet())
			{
				// We call OpenAndProcessExceptions because we want to ensure that the ConnectionRetryPolicy transient error detection 
				// strategies only have to deal with exceptions that have already been wrapped as a PimodSqlExceptions. If we didn't do this
				// the error detection strategies would need to handle .NET SqlClient exceptions.
				//
				if (doNotRetry || _pimodConnectionInfo.ConnectionRetryPolicyInfo == null)
				{
					OpenAndProcessExceptions(() => { DbConnection.Open(); });
				}
				else
				{
					ExecuteWithRetry(_pimodConnectionInfo.ConnectionRetryPolicyInfo,
						() => { OpenAndProcessExceptions(() => { DbConnection.Open(); }); },
						OnConnectionRetrying,
						OnRetryableActionEnded);
				}

				// Set session to null so it will get (re)initialized next time it is accessed.
				//
				_session = null;

				// If if the spid != 0 then it means that it had been initialized previously so we need to make sure to re-initialize it now
				// instead of waiting for it to be lazily initialized (a requirement driven by stress requirement). 
				//
				if (_spid != 0)
				{
					InitializeSpid();
				}
			}
		}

		/// <summary>
		/// Opens the connection using the specified <see cref="Action"/>. Any exceptions that are thrown 
		/// are caught, cast, handled, and re-thrown in the derived types.
		/// </summary>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "The exception type is properly cast, handled, and re-thrown in the derived types.")]
		private void OpenAndProcessExceptions(Action openConnectionAction)
		{
			OnConnectionOpening();

			bool succeeded = false;
			Stopwatch logOnTimer = Stopwatch.StartNew();
			try
			{
				openConnectionAction();
				logOnTimer.Stop();
				succeeded = true;
			}
			catch (Exception e)
			{
				// Unfortunately we don't have a chance to convert the exception to PimodSqlException before raising the event.
				// After we enter the OnExceptionReceived routine it's hard to tell whether the exception comes from a login activity or a regular query.
				//
				logOnTimer.Stop();
				OnConnectionOpenFailed(e, logOnTimer.Elapsed, this);
				OnExceptionReceived(e);
			}

			if (succeeded)
			{
				OnConnectionOpened(logOnTimer.Elapsed);
			}
		}

		/// <summary>
		/// Unsubscribes all listeners from an event with args to avoid possible memory leaks upon dispose.
		/// </summary>
		/// <param name="eventHandler">The event from which to unsubscribe all listeners.</param>
		private void UnsubscribeListenersFromEvent<TEvent>(ref EventHandler<TEvent> eventHandler) where TEvent : EventArgs
		{
			// Unhook any listeners to prevent possible memory leaks.
			//
			if (eventHandler != null)
			{
				foreach (EventHandler<TEvent> eventDelegate in eventHandler.GetInvocationList())
				{
					eventHandler -= eventDelegate;
				}

				eventHandler = null;
			}
		}

		/// <summary>
		/// Unsubscribes all listeners from an event without args to avoid possible memory leaks upon dispose.
		/// </summary>
		/// <param name="eventHandler">The event from which to unsubscribe all listeners.</param>
		private void UnsubscribeListenersFromEvent(ref EventHandler eventHandler)
		{
			// Unhook any listeners to prevent possible memory leaks.
			//
			if (eventHandler != null)
			{
				foreach (EventHandler eventDelegate in eventHandler.GetInvocationList())
				{
					eventHandler -= eventDelegate;
				}

				eventHandler = null;
			}
		}

		/// <summary>
		/// Unsubscribes from all events and listeners to avoid possible memory leaks upon dispose.
		/// </summary>
		private void UnsubscribeFromAllEvents()
		{
			UnsubscribeListenersFromEvent(ref NonQueryExecuted);
			UnsubscribeListenersFromEvent(ref DataReaderExecuted);
			UnsubscribeListenersFromEvent(ref CommandExecuting);
			UnsubscribeListenersFromEvent(ref DatabaseChanging);
			UnsubscribeListenersFromEvent(ref TransactionBeginning);
			UnsubscribeListenersFromEvent(ref TransactionCommitting);
			UnsubscribeListenersFromEvent(ref TransactionRollingBack);
			UnsubscribeListenersFromEvent(ref TransactionSaving);
			UnsubscribeListenersFromEvent(ref PimodSqlErrorsReceived);
			UnsubscribeListenersFromEvent(ref PimodSqlExceptionReceived);
			UnsubscribeListenersFromEvent(ref CommandNowExecuting);
			UnsubscribeListenersFromEvent(ref CommandRetryOccurred);
			UnsubscribeListenersFromEvent(ref RetryableActionEnded);
			UnsubscribeListenersFromEvent(ref ConnectionRetryOccurred);
			UnsubscribeListenersFromEvent(ref ConnectionReopening);
			UnsubscribeListenersFromEvent(ref ConnectionOpening);
			UnsubscribeListenersFromEvent(ref ConnectionOpened);
			UnsubscribeListenersFromEvent(ref ConnectionOpenFailed);
		}
		#endregion Misc Methods

		#region Execute Methods

		#region Dev Notes
		// The ExecuteX() methods have the same pattern:
		//  - Switch database before raising event to avoid logic in client code to handle different cases of databases.  If the client needs to know the database, it can use PimodConnection.CurrentDatabase
		//  - Raise event before creating SqlCommand so that client code can alter the PimodCommandInfo
		//
		#endregion Dev Notes

		#region CreateDbCommand
		/// <summary>
		/// Creates a <see cref="IDbCommand"/> to execute the <paramref name="commandInfo"/> on the implemented connection type
		/// </summary>
		internal abstract IDbCommand CreateDbCommand(PimodCommandInfo commandInfo);
		#endregion

		#region ExecuteScalar

		/* 
		 * NOTES:
		 *			1.	Add other ExecuteScalar overloads with command type, behavior, params etc.. 
		 *				Essentially, this will be a subset of the PimodCommand constructors. 
		 *
		 *			2.	All overloads of ExecuteScalar will construct the PimodCommand and call into
		 *				the ExecuteScalar with PimodCommand
		 *           
		 *			3.	ExecuteScalar is just a helper method for the client to get a single value.  
		 *				Just like within SqlClient, this calls into ExecuteReader.  This also
		 *				allows transforms on events to properly remove any pieces from the reader so
		 *				that client code does not break.  Without doing this, ExecuteScalar()'s client
		 *				code would break if such transforms are done.
		 * */

		/// <summary>
		///		Executes the command, and returns the first column of the first row in the result set returned by the query.  Additional columns or rows are ignored.
		///		<remarks>
		///			<para>All warning messages from the server will be lost.</para>
		///			<para>ExecuteScalar is just a helper method to call ExecuteReader.  The <see cref="DataReaderExecuted"/> event will be fired as a result of this method.</para>
		///		</remarks>
		/// </summary>
		/// <param name="commandText">Command text be executed</param>
		/// <returns>
		/// First column of the first row in the result set returned by the execution of the query
		/// </returns>
		/// <exception cref="PimodSqlException"><see cref="SqlException"/>s will be rethrown as <see cref="PimodSqlException"/></exception>
		public PimodValue ExecuteScalar(string commandText)
		{
			return ExecuteScalar(new PimodCommandInfo(commandText));
		}

		/// <summary>
		///		Executes the command, and returns the first column of the first row in the result set returned by the query.  Additional columns or rows are ignored.
		///		<remarks>
		///			<para>All warning messages from the server will be lost.</para>
		///			<para>ExecuteScalar is just a helper method to call ExecuteReader.  The <see cref="DataReaderExecuted"/> event will be fired as a result of this method.</para>
		///		</remarks>
		/// </summary>
		/// <param name="commandInfo"><see cref="PimodCommandInfo"/> to be executed</param>
		/// <returns>
		/// First column of the first row in the result set returned by the execution of the query
		/// </returns>
		/// <exception cref="PimodSqlException"><see cref="SqlException"/>s will be rethrown as <see cref="PimodSqlException"/></exception>
		public PimodValue ExecuteScalar(PimodCommandInfo commandInfo)
		{
			return ExecuteFunction(() =>
			{
				PimodValue result = null;

				using (PimodDataReader reader = ExecuteReader(commandInfo, _pimodConnectionInfo.DefaultPimodDataReaderBehavior, null, null, true))
				{
					if (reader.Read())
					{
						result = reader.GetPimodValue(0);
					}

					return result;
				}
			});
		}

		#endregion ExecuteScalar

		#region ExecuteNonQuery

		/* 
		 * NOTES:
		 *        1. Add other ExecuteNonQuery overloads with command type, behavior, params etc.. 
		 *           Essentially, this will be a subset of the PimodCommand constructors. 
		 *
		 *         2. All overloads of ExecuteNonQuery will construct the PimodCommand and call into
		 *           the ExecuteNonQuery with PimodCommand
		 * */

		/// <summary>
		/// <para>
		/// Executes a Transact-SQL statement on the server and returns the rowcount and the information messages as 
		///  <see cref="NonQueryResult"/>.
		/// </para>
		/// </summary>
		/// <param name="commandText">Command text to be executed</param>
		/// <returns>Results from the non query execution</returns>
		/// <exception cref="PimodSqlException"><see cref="SqlException"/>s will be rethrown as <see cref="PimodSqlException"/></exception>
		public NonQueryResult ExecuteNonQuery(string commandText)
		{
			return ExecuteNonQuery(new PimodCommandInfo(commandText));
		}

		/// <summary>
		/// <para>
		/// Executes a Transact-SQL statement on the server and returns the rowcount and the information messages as 
		///  <see cref="NonQueryResult"/>.
		/// </para>
		/// </summary>
		/// <param name="commandInfo"><see cref="PimodCommandInfo"/> to be executed</param>
		/// <returns>Results from the non query execution</returns>
		/// <exception cref="PimodSqlException"><see cref="SqlException"/>s will be rethrown as <see cref="PimodSqlException"/></exception>
		public NonQueryResult ExecuteNonQuery(PimodCommandInfo commandInfo)
		{
			// OnExecutingCommand needs to come before connectionContext, as it may be changed by an EventHandler.  
			//
			OnCommandExecuting(commandInfo);

			Stopwatch s = Stopwatch.StartNew();
			using (PimodCommandContext commandContext = PimodCommandContext.Create(this, commandInfo, null, null))
			{
				NonQueryResult result = ShouldRetryCommand()
											? ExecuteNonQueryWithRetry(commandInfo, commandContext)
											: ExecuteNonQueryUsingCommandContext(commandInfo, commandContext);

				OnNonQueryExecuted(commandInfo, result, s.Elapsed);

				return result;
			}
		}

		private NonQueryResult ExecuteNonQueryUsingCommandContext(PimodCommandInfo commandInfo, PimodCommandContext commandContext)
		{
			commandContext.SetContext();
			int rowCount = ExecuteNonQueryInternal(commandInfo);
			return new NonQueryResult(rowCount, commandContext.Errors);
		}

		/// <summary>
		/// Begins ExecuteNonQuery asynchronously and returns immediately.
		/// You must call EndExecute to complete the asynchronous execution.
		/// </summary>
		/// <param name="commandInfo">The command info.</param>
		/// <param name="numberOfRowsAffectedCallback">The number of rows affected callback.</param>
		/// <param name="postExceptionCallback">The post exception callback. Default is null.</param>
		public void BeginExecuteNonQuery(PimodCommandInfo commandInfo, Action<NonQueryResult> numberOfRowsAffectedCallback, Action<Exception> postExceptionCallback = null)
		{
			ThrowIfAsyncExists();
			_asyncThread = new ThreadWithExceptionHandler(
				() => numberOfRowsAffectedCallback(ExecuteNonQuery(commandInfo)),
				e =>
				{
					if (postExceptionCallback != null)
					{
						postExceptionCallback(e);
					}
				});
			_asyncThread.Start();
		}

		/// <summary>
		/// Internal helper method to execute the non query command.
		/// </summary>
		protected abstract int ExecuteNonQueryInternal(PimodCommandInfo commandInfo, bool suppressExecutingEvent = false);

		private NonQueryResult ExecuteNonQueryWithRetry(PimodCommandInfo commandInfo, PimodCommandContext context)
		{
			ReopenConnectionIfClosed();

			return ExecuteWithRetry(
				_pimodConnectionInfo.CommandRetryPolicyInfo,
				() => ExecuteNonQueryUsingCommandContext(commandInfo, context),
				null,
				OnCommandRetrying,
				OnRetryableActionEnded);
		}
		#endregion ExecuteNonQuery

		#region ExecuteReader

		/* 
		 * NOTES:
		 *        1. Add other ExecuteReader overloads with command type, behavior, params etc.. 
		 *           Essentially, this will be a subset of the PimodCommand constructors. 
		 *
		 *         2. All overloads of ExecuteReader will construct the PimodCommand and call into
		 *           the ExecuteReader with PimodCommand
		 * */

		/// <summary>
		/// Executes the given command and returns an active <see cref="PimodDataReader"/>
		/// </summary>
		/// <param name="commandText">Command text to be executed</param>
		/// <returns>Data reader</returns>
		/// <remarks>The caller of the function should manage the reader object returned.</remarks>
		/// <exception cref="PimodSqlException"><see cref="SqlException"/>s will be rethrown as <see cref="PimodSqlException"/></exception>
		public virtual PimodDataReader ExecuteReader(string commandText)
		{
			return ExecuteReader(new PimodCommandInfo(commandText), _pimodConnectionInfo.DefaultPimodDataReaderBehavior);
		}

		/// <summary>
		/// Executes the given command and returns an active <see cref="PimodDataReader"/>.  The <see cref="PimodDataReader"/> may be buffered and resettable depending on the passed in <see cref="PimodDataReaderBehavior"/>.
		/// </summary>
		/// <param name="commandText">Command text to be executed</param>
		/// <param name="readerBehavior"><see cref="PimodDataReaderBehavior"/> to be used.</param>
		/// <returns>Data reader</returns>
		/// <remarks>The caller of the function should manage the reader object returned.</remarks>
		/// <exception cref="PimodSqlException"><see cref="SqlException"/>s will be rethrown as <see cref="PimodSqlException"/></exception>
		public PimodDataReader ExecuteReader(string commandText, PimodDataReaderBehavior readerBehavior)
		{
			return ExecuteReader(new PimodCommandInfo(commandText), readerBehavior);
		}

		/// <summary>
		/// Executes the given command and returns an active <see cref="PimodDataReader"/>
		/// </summary>
		/// <param name="commandInfo"><see cref="PimodCommandInfo"/> to be executed</param>
		/// <returns>Data reader</returns>
		/// <remarks> The caller of the function should manage the reader object returned.</remarks>
		/// <exception cref="PimodSqlException"><see cref="SqlException"/>s will be rethrown as <see cref="PimodSqlException"/></exception>
		public PimodDataReader ExecuteReader(PimodCommandInfo commandInfo)
		{
			return ExecuteReader(commandInfo, _pimodConnectionInfo.DefaultPimodDataReaderBehavior, null, null);
		}

		/// <summary>
		/// Begins ExecuteReader asynchronously and returns immediately.  Reader will be executed with BufferedInMemory and Delayed Exceptions.
		/// You must call EndExecute to complete the asynchronous execution.
		/// </summary>
		/// <param name="commandInfo">The command info.</param>
		/// <param name="pimodDataReaderCallback">The pimod data reader callback that will be called when execution completes.</param>
		/// <exception cref="InvalidOperationException">Thrown if there is already an active asynchronous execution in progress.</exception>
		/// <remarks>There is no postExceptionCallback like other async methods because exceptions are always delayed and will be only 
		/// thrown when the PimodDataReader is consumed. Therefore exception handling should be done in the <paramref name="pimodDataReaderCallback"/>.</remarks>
		public void BeginExecuteReader(PimodCommandInfo commandInfo, Action<PimodDataReader> pimodDataReaderCallback)
		{
			ThrowIfAsyncExists();
			_asyncThread = new ThreadWithExceptionHandler(() => pimodDataReaderCallback(ExecuteReader(commandInfo, PimodDataReaderBehavior.BufferedInMemory)));
			_asyncThread.Start();
		}

		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "CommandContext is disposed of by the returned PimodDataReader.")]
		internal PimodDataReader ExecuteReader(PimodCommandInfo commandInfo, PimodDataReaderBehavior readerBehavior, ManagedPimodConnectionInternal managedPimodConnection, PimodSchemaTable cachedSchemaTable, bool doNotRetry = false)
		{
			// OnExecutingCommand needs to come before connectionContext, as it may be changed by an EventHandler
			//
			OnCommandExecuting(commandInfo);

			// When returning a reader, the reader must own disposing the DatabaseContext since ChangeDatabase() cannot be called
			// when there is an active reader
			//
			PimodDataReader pimodDataReader;
			PimodCommandContext commandContext = PimodCommandContext.Create(this, commandInfo, managedPimodConnection, cachedSchemaTable);

			// Stopwatch for tracking execution time
			//
			Stopwatch s = Stopwatch.StartNew();

			try
			{
				pimodDataReader = !doNotRetry && ShouldRetryCommand()
									? ExecuteReaderWithRetry(commandInfo, commandContext, readerBehavior)
									: ExecuteReaderUsingCommandContext(commandInfo, commandContext, readerBehavior);
			}
			catch
			{
				// Check for a zombied transaction before disposing the command context
				//
				CheckForZombieTransaction();
				commandContext.Dispose();
				throw;
			}

			try
			{
				OnDataReaderExecuted(commandInfo, pimodDataReader, s.Elapsed);
			}
			catch
			{
				pimodDataReader.Dispose();
				throw;
			}

			return pimodDataReader;
		}

		/// <summary>
		/// Run provided function with retry.
		/// </summary>
		/// <typeparam name="T">Return value of the function</typeparam>
		/// <param name="managedPimodConnection">Managed connection to pass to the function</param>
		/// <param name="func">The function to run</param>
		/// <returns>Whatever the function provided returns upon success</returns>
		internal T ExecuteFunctionWithRetry<T>(ManagedPimodConnectionInternal managedPimodConnection, Func<ManagedPimodConnectionInternal, T> func)
		{
			return ExecuteWithRetry(
				_pimodConnectionInfo.CommandRetryPolicyInfo,
				() => func(managedPimodConnection),
				null,
				OnCommandRetrying,
				OnRetryableActionEnded);
		}

		/// <summary>
		/// Run provided function. Retry, if needed.
		/// </summary>
		/// <typeparam name="T">Return value of the function</typeparam>
		/// <param name="func">The function to run</param>
		/// <returns>Whatever the function provided returns upon success</returns>
		public T ExecuteFunction<T>(Func<T> func)
		{
			return ShouldRetryCommand() ? ExecuteFunctionWithRetry(func) : func();
		}

		/// <summary>
		/// Run provided function. Retry, if needed. Before the function is run the connection is reopened as needed.
		/// </summary>
		/// <typeparam name="T">Return value of the function</typeparam>
		/// <param name="func">The function to run</param>
		/// <returns>Whatever the function provided returns upon success</returns>
		internal T ExecuteFunctionWithRetry<T>(Func<T> func)
		{
			ReopenConnectionIfClosed();

			return ExecuteWithRetry(
				_pimodConnectionInfo.CommandRetryPolicyInfo,
				func,
				null,
				OnCommandRetrying,
				OnRetryableActionEnded);
		}

		/// <summary>
		/// Executes the given command and returns a <see cref="PimodDataReader"/>.  The <see cref="PimodDataReader"/> may be buffered and resettable depending on the passed in <see cref="PimodDataReaderBehavior"/>.
		/// </summary>
		/// <param name="commandInfo"><see cref="PimodCommandInfo"/> to be executed</param>
		/// <param name="readerBehavior"><see cref="PimodDataReaderBehavior"/> to be used.</param>
		/// <exception cref="PimodSqlException"><see cref="SqlException"/>s will be rethrown as <see cref="PimodSqlException"/></exception>
		public PimodDataReader ExecuteReader(PimodCommandInfo commandInfo, PimodDataReaderBehavior readerBehavior)
		{
			return ExecuteReader(commandInfo, readerBehavior, null, null);
		}

		private PimodDataReader ExecuteReaderUsingCommandContext(PimodCommandInfo commandInfo, PimodCommandContext context, PimodDataReaderBehavior dataReaderBehavior)
		{
			context.SetContext();
			return ExecuteReaderInternal(commandInfo, context, dataReaderBehavior);
		}

		private PimodDataReader ExecuteReaderWithRetry(PimodCommandInfo commandInfo, PimodCommandContext context, PimodDataReaderBehavior readerBehavior)
		{
			ReopenConnectionIfClosed();

			return ExecuteWithRetry(
				_pimodConnectionInfo.CommandRetryPolicyInfo,
				() => ExecuteReaderUsingCommandContext(commandInfo, context, readerBehavior),
				pimodDataReader => pimodDataReader.ImmediateDelayedException,
				OnCommandRetrying,
				OnRetryableActionEnded);
		}

		internal abstract PimodDataReader ExecuteReaderInternal(PimodCommandInfo commandInfo, PimodCommandContext connectionContext, PimodDataReaderBehavior readerBehavior);


		#endregion ExecuteReader

		#region Asynchronous

		/// <summary>
		/// Blocks until the active Execution completes.
		/// </summary>
		/// <exception cref="InvalidOperationException">Thrown if there is no asynchronous execution in progress.</exception>
		public void EndExecute()
		{
			if (_asyncThread == null)
			{
				throw new InvalidOperationException("Must call BeginExecute* before calling EndExecute.");
			}
			try
			{
				_asyncThread.Join();
			}
			finally
			{
				_asyncThread = null;
			}
		}

		private void ThrowIfAsyncExists()
		{
			if (_asyncThread != null)
			{
				throw new InvalidOperationException("Must call EndExecute* before beginning a new asynchronous execution.");
			}
		}
		#endregion

		#region OnExceptionReceived
		/// <summary>
		/// Implemented by each class to raise the provider specific exception as a <see cref="PimodSqlException"/>
		/// </summary>
		/// <param name="exception"></param>
		internal abstract void OnExceptionReceived(Exception exception);
		#endregion

		#region ToggleInfoMessageEventing
		/// <summary>
		/// Enables or diables info message eventing.  
		/// </summary>
		/// <remarks>
		/// This will be toggled off for performance testing purposes
		/// </remarks>
		/// <param name="enable">If true, enables the event.  Otherwise, disables the event.</param>
		internal abstract void ToggleInfoMessageEventing(bool enable);
		#endregion

		#endregion Execute Methods

		#region Dispose
		/// <summary>
		/// Disposes and thus closes the connection
		/// </summary>
		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		/// <summary>
		/// Disposes and thus closes the connection
		/// </summary>
		protected virtual void Dispose(bool disposing)
		{
			if (disposing)
			{
				if (!_disposed)
				{
					// If pooling is enabled, reset the database to master before disposing.  
					// Not doing so in standalone environment can lead to confusing behavior since
					// the database cannot be dropped since it is in use by the pooled connection
					//
					if (_pimodConnectionInfo.UsePooling && !this.DatabaseName.Equals("master") && DbConnection.State == ConnectionState.Open && !IsCloud)
					{
						ChangeDatabase("master");
					}
					if (_asyncThread != null)
					{
						_asyncThread.Dispose();
					}
					UnsubscribeFromAllEvents();
					_sqlInstance = null;
					_disposed = true;
				}
			}
		}

		internal bool IsDisposed
		{
			get { return _disposed; }
		}
		#endregion

		#region Protected and Private Event Raisers
		/// <summary>
		/// Raises the <see cref="CommandRetryOccurred"/> event.
		/// </summary>
		private void OnCommandRetrying(object sender, RetryOccurredEventArgs eventArgs)
		{
			if (CommandRetryOccurred != null)
			{
				CommandRetryOccurred(this, eventArgs);
			}

			// If a command was executed that resulted in a transient error, it's possible the connection was closed,
			// in which case we want to re-open it.
			//
			if (eventArgs.ConnectionShouldBeOpen)
			{
				ReopenConnectionIfClosed();
			}
		}

		/// <summary>
		/// Raises the <see cref="ConnectionRetryOccurred"/> event.
		/// </summary>
		private void OnRetryableActionEnded(object sender, RetryableActionEndedEventArgs eventArgs)
		{
			if (RetryableActionEnded != null)
			{
				RetryableActionEnded(this, eventArgs);
			}
		}

		/// <summary>
		/// Raises the <see cref="ConnectionRetryOccurred"/> event.
		/// </summary>
		private void OnConnectionRetrying(object sender, RetryOccurredEventArgs eventArgs)
		{
			if (ConnectionRetryOccurred != null)
			{
				ConnectionRetryOccurred(this, eventArgs);
			}
		}

		/// <summary>
		/// Raises the <see cref="ConnectionReopening"/> event.
		/// </summary>
		private void OnConnectionReopening()
		{
			if (ConnectionReopening != null)
			{
				ConnectionReopening(this, null);
			}
		}

		/// <summary>
		/// Raises the <see cref="PimodSqlExceptionReceived" /> event after creating a <see cref="PimodSqlException" /> from the <see cref="SqlException" />
		/// </summary>
		/// <param name="pimodSqlException">The pimod SQL exception.</param>
		/// <exception cref="PimodSqlException">Throws a <see cref="PimodSqlException" /> as long as the event handlers do not request it to be swallowed.</exception>
		internal void OnPimodSqlExceptionReceived(PimodSqlException pimodSqlException)
		{
			bool throwException = true;

			if (this.PimodSqlExceptionReceived != null)
			{
				PimodSqlExceptionReceivedEventArgs eventArgs = new PimodSqlExceptionReceivedEventArgs(pimodSqlException);
				this.PimodSqlExceptionReceived(this, eventArgs);
				throwException = !eventArgs.SwallowException;
			}

			if (throwException)
			{
				throw pimodSqlException;
			}
		}

		/// <summary>
		/// Raises the <see cref="CommandExecuting"/> event
		/// </summary>
		/// <param name="commandInfo">The command that is being executed</param>
		protected void OnCommandExecuting(PimodCommandInfo commandInfo)
		{
			if (this.CommandExecuting != null)
			{
				this.CommandExecuting(this, new CommandExecutingEventArgs(commandInfo, this.DatabaseName, this.UniqueId, null));
			}
		}

		/// <summary>
		/// Raises the <see cref="CommandFailed" /> event.
		/// </summary>
		/// <param name="exception">The exception that caused the command failure.</param>
		/// <param name="elapsedTime">The elapsed time from operation (command) started to exception encountered.</param>
		/// <param name="command">The command information.</param>
		protected void OnCommandFailed(Exception exception, TimeSpan elapsedTime, PimodCommandInfo command)
		{
			if (CommandFailed != null)
			{
				CommandFailedEventArgs eventArgs = new CommandFailedEventArgs(exception, elapsedTime, command);
				this.CommandFailed(this, eventArgs);
			}
		}

		/// <summary>
		/// Raises the <see cref="NonQueryExecuted" /> event
		/// </summary>
		/// <param name="command">The command that was executed</param>
		/// <param name="nonQueryResult">Results returned from the execution of the command</param>
		/// <param name="elapsedTime">The elapsed time.</param>
		protected void OnNonQueryExecuted(PimodCommandInfo command, NonQueryResult nonQueryResult, TimeSpan elapsedTime)
		{
			if (this.NonQueryExecuted != null)
			{
				this.NonQueryExecuted(this, new NonQueryExecutedEventArgs(command, nonQueryResult, this.DatabaseName, elapsedTime));
			}
		}

		/// <summary>
		/// Raises the <see cref="DataReaderExecuted" /> event
		/// </summary>
		/// <param name="command">The command that was executed</param>
		/// <param name="pimodDataReader">SqlDataReader returned from the execution of the command</param>
		/// <param name="elapsedTime">The elapsed time.</param>
		protected void OnDataReaderExecuted(PimodCommandInfo command, PimodDataReader pimodDataReader, TimeSpan elapsedTime)
		{
			if (this.DataReaderExecuted != null)
			{
				this.DataReaderExecuted(this, new DataReaderExecutedEventArgs(command, pimodDataReader, this.DatabaseName, elapsedTime));
			}
		}

		/// <summary>
		/// Raises the <see cref="DatabaseChanging"/> event
		/// </summary>
		/// <param name="previousDatabaseName">The name of the database to change from</param>
		/// <param name="newDatabaseName">The name of the database to change into</param>
		protected void OnDatabaseChanging(string previousDatabaseName, string newDatabaseName)
		{
			// We could make ChangeDatabaseEventArgs read/write and then update this method to return the resulting DatabaseName (and then 
			// use that within _sqlConnection.ChangeDatabase().
			// This would allow event handlers to redirect change database requests.
			//
			if (this.DatabaseChanging != null)
			{
				this.DatabaseChanging(this, new DatabaseChangingEventArgs(previousDatabaseName, newDatabaseName, this.UniqueId, null));
			}
		}

		/// <summary>
		/// Raises the <see cref="CommandExecuting"/> event
		/// </summary>
		/// <param name="commandInfo">The command that is being executed</param>
		internal void OnCommandNowExecuting(PimodCommandInfo commandInfo)
		{
			if (this.CommandNowExecuting != null)
			{
				this.CommandNowExecuting(this, new CommandExecutingEventArgs(commandInfo, this.DatabaseName, this.UniqueId, null));
			}
		}

		/// <summary>
		/// Raises the <see cref="PimodConnectionCreated"/> event.
		/// </summary>
		protected internal static void OnPimodConnectionCreated(PimodConnection pimodConnection, TimeSpan timeToCreate)
		{
			if (PimodConnectionCreated != null)
			{
				PimodConnectionCreated(null, new PimodConnectionCreatedEventArgs(pimodConnection, timeToCreate));
			}
		}

		/// <summary>
		/// Raises the <see cref="PimodConnectionCreating"/> event.
		/// </summary>
		/// <param name="pimodConnectionInfo">The pimod connection info.</param>
		protected internal static void OnPimodConnectionCreating(PimodConnectionInfo pimodConnectionInfo)
		{
			if (PimodConnectionCreating != null)
			{
				PimodConnectionCreating(null, new PimodConnectionCreatingEventArgs(pimodConnectionInfo));
			}
		}

		/// <summary>
		/// Raises the <see cref="PimodConnectionOpeningDuringCreate"/> event.
		/// </summary>
		/// <param name="pimodConnection">The pimod connection.</param>
		protected internal static void OnPimodConnectionOpeningDuringCreate(PimodConnection pimodConnection)
		{
			if (PimodConnectionOpeningDuringCreate != null)
			{
				PimodConnectionOpeningDuringCreate(null, new PimodConnectionOpeningDuringCreateEventArgs(pimodConnection));
			}
		}

		/// <summary>
		/// Raises the <see cref="ConnectionOpening"/> event.
		/// </summary>
		protected internal void OnConnectionOpening()
		{
			if (ConnectionOpening != null)
			{
				ConnectionOpening(this, null);
			}
		}

		/// <summary>
		/// Raises the <see cref="ConnectionOpened"/> event.
		/// </summary>
		protected internal void OnConnectionOpened(TimeSpan timeToComplete)
		{
			if (ConnectionOpened != null)
			{
				ConnectionOpened(this, new ConnectionOpenedEventArgs(timeToComplete));
			}
		}

		/// <summary>
		/// Raises the <see cref="ConnectionOpenFailed"/> event.
		/// </summary>
		/// <param name="exception">The exception thrown when connection open failed</param>
		/// <param name="timeToFail">The time taken to fail. </param>
		/// <param name="pimodConn">The pimod connection that failed on connection open</param>
		protected internal void OnConnectionOpenFailed(Exception exception, TimeSpan timeToFail, PimodConnection pimodConn)
		{
			if (ConnectionOpenFailed != null)
			{
				ConnectionOpenFailed(this, new ConnectionOpenFailedEventArgs(exception, timeToFail, pimodConn));
			}
		}

		/// <summary>
		/// Raises the <see cref="TransactionBeginning"/> event
		/// </summary>
		/// <param name="isolationLevel">Isolation level</param>
		/// <param name="transactionName">Transaction name</param>
		protected void OnTransactionBeginning(IsolationLevel isolationLevel, string transactionName)
		{
			if (this.TransactionBeginning != null)
			{
				this.TransactionBeginning(this, new TransactionBeginningEventArgs(isolationLevel, transactionName, this.UniqueId, null));
			}
		}

		/// <summary>
		/// Raises the <see cref="TransactionCommitting"/> event
		/// </summary>
		protected void OnTransactionCommitting()
		{
			if (this.TransactionCommitting != null)
			{
				this.TransactionCommitting(this, new TransactionCommittingEventArgs(this.UniqueId, null));
			}
		}

		/// <summary>
		/// Raises the <see cref="TransactionRollingBack"/> event
		/// </summary>
		/// <param name="transactionName">Transaction name</param>
		protected void OnTransactionRollingBack(string transactionName)
		{
			if (this.TransactionRollingBack != null)
			{
				this.TransactionRollingBack(this, new TransactionRollingBackEventArgs(transactionName, this.UniqueId, null));
			}
		}

		/// <summary>
		/// Raises the <see cref="TransactionSaving"/> event
		/// </summary>
		/// <param name="transactionName">Transaction name</param>
		protected void OnTransactionSaving(string transactionName)
		{
			if (this.TransactionSaving != null)
			{
				this.TransactionSaving(this, new TransactionSavingEventArgs(transactionName, this.UniqueId, null));
			}
		}
		#endregion Protected Event Raisers

		#region Information Message Handling


		/// <summary>
		/// Called when an error is received on the connection.
		/// </summary>
		/// <param name="errors">The errors.</param>
		protected void OnPimodSqlErrorsReceived(ReadOnlyCollection<PimodSqlError> errors)
		{
			if (this.PimodSqlErrorsReceived != null)
			{
				PimodSqlErrorsReceivedEventArgs newEventArgs = PimodSqlErrorsReceivedEventArgs.Create(errors);
				this.PimodSqlErrorsReceived(this, newEventArgs);
			}
		}

		#endregion
	}

	internal class PimodSqlConnection : PimodConnection
	{
		#region Fields

		private static readonly Dictionary<SqlDataType, SqlDbType> _sqlDbTypeMap = GetSqlDbTypeMap();

		private SqlCommand _currentSqlCommand;
		private List<SqlCommand> _currentSqlCommands;
		private SqlConnection _sqlConnection;
		private SqlTransaction _sqlTransaction;
		private static readonly PropertyInfo _securityTokenPropertyInfo = (typeof(SqlConnection)).GetProperty("SecurityToken");
		#endregion

		#region Constructors

		public PimodSqlConnection(PimodConnectionInfo pimodConnectionInfo)
			: base(pimodConnectionInfo)
		{
			_sqlConnection = new SqlConnection(CreateSqlClientConnectionString(pimodConnectionInfo));
			if (pimodConnectionInfo.SecurityToken != null)
			{
				if (_securityTokenPropertyInfo == null)
				{
					throw new PimodValidationException("SecurityToken property is expected in the SqlConnection Object in case SecurityToken is provided in the PimodConnectionInfo. " +
													   "It is in the patched version of .NET 4.5. Make sure you have it installed.");
				}
				var securityTokenSecureString = new SecureString();
				foreach (char c in pimodConnectionInfo.SecurityToken)
				{
					securityTokenSecureString.AppendChar(c);
				}
				securityTokenSecureString.MakeReadOnly();

				_securityTokenPropertyInfo.SetValue(_sqlConnection, securityTokenSecureString, null);
			}

			if (!string.IsNullOrEmpty(pimodConnectionInfo.AccessToken))
			{
				_sqlConnection.AccessToken = pimodConnectionInfo.AccessToken;
			}

			// If MARS is enabled, we need to keep a list of commands.  Set that up.  Otherwise, we just need to hang on to the current one (for Close())
			//
			if (pimodConnectionInfo.EnableMultipleActiveResultSets)
			{
				_currentSqlCommands = new List<SqlCommand>();
			}
		}

		#endregion

		#region Properties

		/// <summary>
		/// Gets the generic db connection.
		/// </summary>
		/// <value>The db connection.</value>
		protected override IDbConnection DbConnection
		{
			get { return _sqlConnection; }
		}

		/// <summary>
		/// Gets the generic db transaction.
		/// </summary>
		/// <value>The db transaction.</value>
		protected internal override IDbTransaction DbTransaction
		{
			get { return _sqlTransaction; }
		}

		/// <summary>
		/// Gets the SqlConnection managed by this object
		/// </summary>
		/// <value>The SQL connection.</value>
		internal SqlConnection SqlConnection
		{
			get { return _sqlConnection; }
		}

		internal static Dictionary<SqlDataType, SqlDbType> SqlDbTypeMap
		{
			get { return _sqlDbTypeMap; }
		}

		public override bool SupportsNamedTransactions
		{
			get { return true; }
		}

		public override bool SupportsSaveTransactions
		{
			get { return true; }
		}

		#endregion

		#region Methods

		protected override void BeginTransactionCore(IsolationLevel isolationLevel, string transactionName)
		{
			// SqlConnection.BeginTransaction(IsolationLevel) calls SqlConnection.BeginTransaction(isolationLevel, null), so it's fine to
			// always call this method even if transactionName is null
			//
			_sqlTransaction = SqlConnection.BeginTransaction(isolationLevel, transactionName);
		}

		/// <summary>
		/// Tries to cancel the command currently being executed.
		/// </summary>
		/// <remarks>
		/// If there is nothing to cancel, nothing occurs.  However, if there is a command in process, and the attempt to cancel fails, no exception is generated. 
		/// If MARS is enabled, a cancel will be attempted on all active commands.
		/// </remarks>
		public override void Cancel()
		{
			// Note: It is always safe to call SqlCommand.Cancel().  If there is not a current execution, it will be a no-op rather than throwing.  This 
			// function continues on that logic
			//
			if (!PimodConnectionInfo.EnableMultipleActiveResultSets)
			{
				if (_currentSqlCommand == null)
				{
					return;
				}

				_currentSqlCommand.Cancel();
			}
			else
			{
				// Lock must be taken since this is list could be accessed on multiple threads between this call and Execute calls
				//
				lock (_currentSqlCommands)
				{
					foreach (SqlCommand command in _currentSqlCommands)
					{
						command.Cancel();
					}
				}
			}
		}

		/// <summary>
		/// Method for underlying PimodConnection implementation to change the database.
		/// </summary>
		/// <param name="databaseName">Name of the database.</param>
		protected override void ChangeDatabaseInternal(string databaseName)
		{
			try
			{
				_sqlConnection.ChangeDatabase(databaseName);
			}
			catch (SqlException e)
			{
				OnSqlExceptionReceived(e);
			}
		}

		/// <summary>
		///  Builds the connection string
		/// </summary>
		internal static string CreateSqlClientConnectionString(PimodConnectionInfo connectionInfo)
		{
			SqlConnectionStringBuilder connectionBuilder = new SqlConnectionStringBuilder();

			connectionBuilder.ApplicationName = connectionInfo.ApplicationName;
			if (connectionInfo.AttachDatabaseFileName != null)
			{
				connectionBuilder.AttachDBFilename = connectionInfo.AttachDatabaseFileName;
			}
			connectionBuilder.ConnectTimeout = connectionInfo.ConnectionTimeout;
			connectionBuilder.DataSource = String.Format("{0}{1}{2}", connectionInfo.UseDedicatedAdministratorConnection ? "admin:" : String.Empty, connectionInfo.ServerName, connectionInfo.Port.HasValue ? "," + connectionInfo.Port.Value : String.Empty);
			connectionBuilder.Encrypt = connectionInfo.Encrypt;

			if (connectionInfo.Enlist.HasValue)
			{
				connectionBuilder.Enlist = connectionInfo.Enlist.Value;
			}

			connectionBuilder.InitialCatalog = connectionInfo.DatabaseName;
			connectionBuilder.MultipleActiveResultSets = connectionInfo.EnableMultipleActiveResultSets;
			connectionBuilder.Pooling = connectionInfo.UsePooling;
			connectionBuilder.WorkstationID = connectionInfo.WorkstationId;

			if (connectionInfo.TrustServerCertificate.HasValue)
			{
				connectionBuilder.TrustServerCertificate = connectionInfo.TrustServerCertificate.Value;
			}

			if (connectionInfo.MaxPoolSize.HasValue)
			{
				connectionBuilder.MaxPoolSize = connectionInfo.MaxPoolSize.Value;
			}

			if (connectionInfo.UseIntegratedSecurity)
			{
				connectionBuilder.IntegratedSecurity = true;
			}
			else
			{
				if (connectionInfo.SecurityToken == null && connectionInfo.AccessToken == null)
				{
					connectionBuilder.UserID = connectionInfo.UserName;
					connectionBuilder.Password = connectionInfo.Password;
				}
				connectionBuilder.IntegratedSecurity = false;
			}

			if (!String.IsNullOrEmpty(connectionInfo.ServerSpn))
			{
				connectionBuilder.Add("ServerSPN", connectionInfo.ServerSpn);
			}

			if (connectionInfo.NetworkLibrary.HasValue)
			{
				connectionBuilder.NetworkLibrary = GetSqlClientNetworkLibraryString(connectionInfo.NetworkLibrary.Value);
			}

			if (connectionInfo.ApplicationIntent != null)
			{
				connectionBuilder.Add("ApplicationIntent", connectionInfo.ApplicationIntent.ToString());
			}

			if (connectionBuilder.ContainsKey("TransparentNetworkIpResolution"))
			{
				connectionBuilder["TransparentNetworkIpResolution"] = connectionInfo.UseTransparentNetworkIpResolution;
			}

			// In later .net versions, the ConnectRetryCount (the number of times it will attempt to re-establish the connection automatically) defaults to 1
			// this messes with pimod's internal connection establishing mechanisms. .
			//
			if (connectionBuilder.ContainsKey("ConnectRetryCount"))
			{
				connectionBuilder["ConnectRetryCount"] = connectionInfo.ConnectRetryCount;
				connectionBuilder["ConnectRetryInterval"] = connectionInfo.ConnectRetryInterval;
			}

			string sqlString = connectionBuilder.ToString();

			// Add authentication type which may not be supported in current version of .Net (4.5.2)
			// Will update this code once the latest(4.5.3) .Net is released
			//
			if (connectionInfo.Authentication.HasValue)
			{
				sqlString += String.Format(";Authentication={0}", connectionInfo.Authentication.AsString());
			}
			if (PimodTceSqlConnectionSetting.None != connectionInfo.TceConnectionSetting)
			{
				sqlString += String.Format(";Column Encryption Setting={0}", connectionInfo.TceConnectionSetting.AsString());
			}

			if (!string.IsNullOrWhiteSpace(connectionInfo.EnclaveAttestationUrl))
			{
				sqlString += String.Format(";Enclave Attestation Url={0}", connectionInfo.EnclaveAttestationUrl);
			}
			// We would also need to add Multiple subnet failover property support in future when sql client will support it. 
			//
			return sqlString;
		}

		/// <summary>
		///  Creates a sqlcommand from the command
		/// </summary>
		/// <returns>SqlCommand equivalent of the command object</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:ReviewSqlQueriesForSecurityVulnerabilities", Justification = "Command Info data copied by design", Scope = "method")]
		internal override IDbCommand CreateDbCommand(PimodCommandInfo commandInfo)
		{
			if (commandInfo.IsPrepared)
			{
				IDbCommand dbCommand;
				if (commandInfo.TryGetPreparedCommand(this, out dbCommand))
				{
					if (HasActiveTransaction)
					{
						dbCommand.Transaction = _sqlTransaction;
					}
					return dbCommand;
				}
			}
			SqlCommand result = new SqlCommand();
			try
			{
				if (HasActiveTransaction)
				{
					result.Transaction = _sqlTransaction;
				}
				result.Connection = this.SqlConnection;
				result.CommandText = commandInfo.CommandText;
				result.CommandTimeout = commandInfo.CommandTimeout.HasValue ? commandInfo.CommandTimeout.Value : this.PimodConnectionInfo.DefaultCommandTimeout;
				result.CommandType = commandInfo.CommandType;
				if (commandInfo.Parameters != null)
				{
					foreach (PimodParameter pimodParameter in commandInfo.Parameters)
					{
						SqlParameter sqlParameter = CreateSqlParameter(pimodParameter);
						if (commandInfo.IsPrepared)
						{
							pimodParameter.PreparedParameters.Add(sqlParameter);
						}
						if (pimodParameter.ParameterDirection != ParameterDirection.Input)
						{
							if (commandInfo.IsPrepared && pimodParameter.LastExecutedParameter != null)
							{
								//DEVNOTE: We do not support executing a prepared command on multiple connections - 
								//because prepared commands cache the underlying DbCommand, there is no good way to track which was the last
								//command to execute, hence which was the last one we should get the OutputValue from
								//
								throw new NotSupportedException("Cannot execute a Prepared Command with Ouput Parameters on multiple different connections.");
							}
							pimodParameter.LastExecutedParameter = sqlParameter;
						}
						result.Parameters.Add(sqlParameter);
						if (pimodParameter.ParameterName == null && pimodParameter.ParameterName != sqlParameter.ParameterName)
						{
							// Ado.net sets a default parameter name when the parameter is added to the collection.
							// Reset it to null to preserve intended behavior for parameter passing.
							//
							sqlParameter.ParameterName = null;
						}
					}
				}
				if (commandInfo.IsPrepared)
				{
					result.Prepare();
					commandInfo.AddPreparedCommand(this, result);
				}
			}
			catch
			{
				result.Dispose();
				throw;
			}
			return result;
		}

		private static SqlParameter CreateSqlParameter(PimodParameter parameter)
		{
			SqlParameter sqlParameter = CreateSqlParameterFromType(parameter.ParameterType);
			sqlParameter.ParameterName = parameter.ParameterName;
			sqlParameter.Direction = parameter.ParameterDirection;

			// Parameter value is null for output parameters.  Null reference values can also be used to use server default values when
			// the CommandType == CommandType.StoredProcedure
			//
			if (parameter.InputValue != null)
			{
				sqlParameter.Value = parameter.InputValue.ToSqlClientValue();
			}

			return sqlParameter;
		}

		private static SqlParameter CreateSqlParameterFromType(PimodType pimodType)
		{
			SqlParameter sqlParameter = new SqlParameter();
			sqlParameter.SqlDbType = _sqlDbTypeMap[pimodType.SystemDataType];
			if (pimodType.SystemDataType == SqlDataType.DateTime2 || pimodType.SystemDataType == SqlDataType.DateTimeOffset || pimodType.SystemDataType == SqlDataType.Time)
			{
				if (pimodType.Scale == 0)
				{
					throw new NotSupportedException("SqlClient currently has a bug where Scale=0 is not supported on SqlParameters for DATETIME2/DATETIMEOFFSET/TIME.  Once VSTS 216147 is fixed, this exception will be removed.");
				}
				sqlParameter.Scale = (byte)pimodType.Scale;
				sqlParameter.Size = pimodType.MaxStorageSize;
			}
			else if (pimodType.SystemDataType == SqlDataType.Decimal || pimodType.SystemDataType == SqlDataType.Numeric)
			{
				sqlParameter.Scale = (byte)pimodType.Scale;
				sqlParameter.Precision = (byte)pimodType.Precision;
				sqlParameter.Size = pimodType.MaxStorageSize;
			}
			else if (pimodType.SystemDataType == SqlDataType.Table)
			{
				sqlParameter.TypeName = pimodType.SchemaQualifiedName;
			}
			else if (pimodType.SystemDataType == SqlDataType.Udt)
			{
				sqlParameter.UdtTypeName = pimodType.ThreePartName;
				sqlParameter.Size = pimodType.MaxStorageSize;
			}
			else if (pimodType is PimodLengthType)
			{
				sqlParameter.Size = pimodType.Length;
			}
			else if (pimodType.IsLob)
			{
				sqlParameter.Size = -1;
			}
			return sqlParameter;
		}

		/// <summary>
		/// Disposes and thus closes the connection
		/// </summary>
		protected override void Dispose(bool disposing)
		{
			bool isDisposed = IsDisposed;
			base.Dispose(disposing);

			if (!isDisposed)
			{
				_sqlConnection.Dispose();
				_sqlConnection.InfoMessage -= SqlConnectionInfoMessage;
			}
		}

		protected override int ExecuteNonQueryInternal(PimodCommandInfo commandInfo, bool suppressExecutingEvent = false)
		{
			int rowCount = 0;
			using (SqlCommand sqlCommand = GetSqlCommand(commandInfo))
			{
				if (!suppressExecutingEvent)
				{
					// Raise the command now executing event to allow loggers to log the actual command being executed.  Do this after the SqlCommand
					// is created to ensure that any changes to the commandInfo do not have an effect.
					//
					OnCommandNowExecuting(commandInfo);
				}

				Stopwatch s = Stopwatch.StartNew();
				try
				{
					rowCount = sqlCommand.ExecuteNonQuery();
				}
				catch (SqlException sqlException)
				{
					OnCommandFailed(sqlException, s.Elapsed, commandInfo);
					OnSqlExceptionReceived(sqlException);
				}
				finally
				{
					CheckForZombieTransaction();
				}

				RemoveSqlCommand(sqlCommand);
			}
			return rowCount;
		}

		internal override PimodDataReader ExecuteReaderInternal(PimodCommandInfo commandInfo, PimodCommandContext commandContext, PimodDataReaderBehavior readerBehavior)
		{
			using (SqlCommand sqlCommand = GetSqlCommand(commandInfo))
			{
				// Raise the command now executing event to allow loggers to log the actual command being executed.  Do this after the SqlCommand
				// is created to ensure that any changes to the commandInfo do not have an effect.
				//
				OnCommandNowExecuting(commandInfo);

				Stopwatch s = Stopwatch.StartNew();
				try
				{
					// Due to the way that sql variants are read, the command must be read sequentially.  The PDR handles and hides this from user
					//
					SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(CommandBehavior.SequentialAccess);
					return PimodDataReader.Create(this, sqlCommand, sqlDataReader, commandContext, readerBehavior,
						commandInfo.ResultRowCountLimit);
				}
				catch (SqlException sqlException)
				{
					try
					{
						OnCommandFailed(sqlException, s.Elapsed, commandInfo);

						// Raise event, which would throw a PimodSqlException that is created from the sqlException.
						//
						OnSqlExceptionReceived(sqlException);
						RemoveSqlCommand(sqlCommand);
					}
					catch (PimodSqlException pse)
					{
						if ((readerBehavior & PimodDataReaderBehavior.DoNotDelayException) == PimodDataReaderBehavior.DoNotDelayException)
						{
							throw;
						}
						CheckForZombieTransaction();
						commandContext.Dispose();
						RemoveSqlCommand(sqlCommand);
						return PimodDataReader.Create(pse, commandContext.Errors);
					}
				}
				return null;
			}
		}

		private SqlCommand GetSqlCommand(PimodCommandInfo command)
		{
			SqlCommand sqlCommand = (SqlCommand)CreateDbCommand(command);

			_sqlConnection.FireInfoMessageEventOnUserErrors = command.TreatErrorsAsMessages.HasValue ? command.TreatErrorsAsMessages.Value : PimodConnectionInfo.DefaultTreatErrorsAsMessages;

			if (!PimodConnectionInfo.EnableMultipleActiveResultSets)
			{
				_currentSqlCommand = sqlCommand;
			}
			else
			{
				// Lock must be taken since this is list could be accessed on multiple threads w/ Cancel() call
				//
				lock (_currentSqlCommands)
				{
					_currentSqlCommands.Add(sqlCommand);
				}
			}
			return sqlCommand;
		}

		public static string GetSqlClientNetworkLibraryString(NetworkLibrary networkLibrary)
		{
			// Give the full type name since we expose NetworkLibrary property
			//
			switch (networkLibrary)
			{
				case Microsoft.SqlServer.Test.Pimod.NetworkLibrary.AppleTalk:
					return "dbmsadsn";
				case Microsoft.SqlServer.Test.Pimod.NetworkLibrary.IpxSpx:
					return "dbmsspxn";
				case Microsoft.SqlServer.Test.Pimod.NetworkLibrary.Multiprotocol:
					return "dbmsrpcn";
				case Microsoft.SqlServer.Test.Pimod.NetworkLibrary.NamedPipes:
					return "dbnmpntw";
				case Microsoft.SqlServer.Test.Pimod.NetworkLibrary.SharedMemory:
					return "dbmslpcn";
				case Microsoft.SqlServer.Test.Pimod.NetworkLibrary.TcpIP:
					return "dbmssocn";
				case Microsoft.SqlServer.Test.Pimod.NetworkLibrary.Via:
					return "dbmsgnet";
				default:
					throw new ArgumentException("BUG IN PIMOD: Unknown NetworkLibrary: " + networkLibrary);
			}
		}

		private static Dictionary<SqlDataType, SqlDbType> GetSqlDbTypeMap()
		{
			Dictionary<SqlDataType, SqlDbType> sqlDbTypeMap = new Dictionary<SqlDataType, SqlDbType>();
			sqlDbTypeMap.Add(SqlDataType.BigInt, SqlDbType.BigInt);
			sqlDbTypeMap.Add(SqlDataType.Binary, SqlDbType.Binary);
			sqlDbTypeMap.Add(SqlDataType.Bit, SqlDbType.Bit);
			sqlDbTypeMap.Add(SqlDataType.Char, SqlDbType.Char);
			sqlDbTypeMap.Add(SqlDataType.Date, SqlDbType.Date);
			sqlDbTypeMap.Add(SqlDataType.DateTime, SqlDbType.DateTime);
			sqlDbTypeMap.Add(SqlDataType.DateTime2, SqlDbType.DateTime2);
			sqlDbTypeMap.Add(SqlDataType.DateTimeOffset, SqlDbType.DateTimeOffset);
			sqlDbTypeMap.Add(SqlDataType.Decimal, SqlDbType.Decimal);
			sqlDbTypeMap.Add(SqlDataType.Float, SqlDbType.Float);
			sqlDbTypeMap.Add(SqlDataType.Image, SqlDbType.Image);
			sqlDbTypeMap.Add(SqlDataType.Int, SqlDbType.Int);
			sqlDbTypeMap.Add(SqlDataType.Money, SqlDbType.Money);
			sqlDbTypeMap.Add(SqlDataType.NChar, SqlDbType.NChar);
			sqlDbTypeMap.Add(SqlDataType.NText, SqlDbType.NText);
			sqlDbTypeMap.Add(SqlDataType.Numeric, SqlDbType.Decimal);
			sqlDbTypeMap.Add(SqlDataType.NVarChar, SqlDbType.NVarChar);
			sqlDbTypeMap.Add(SqlDataType.Real, SqlDbType.Real);
			sqlDbTypeMap.Add(SqlDataType.SmallDateTime, SqlDbType.SmallDateTime);
			sqlDbTypeMap.Add(SqlDataType.SmallInt, SqlDbType.SmallInt);
			sqlDbTypeMap.Add(SqlDataType.SmallMoney, SqlDbType.SmallMoney);
			sqlDbTypeMap.Add(SqlDataType.SqlVariant, SqlDbType.Variant);
			sqlDbTypeMap.Add(SqlDataType.Table, SqlDbType.Structured);
			sqlDbTypeMap.Add(SqlDataType.Text, SqlDbType.Text);
			sqlDbTypeMap.Add(SqlDataType.Time, SqlDbType.Time);
			sqlDbTypeMap.Add(SqlDataType.Timestamp, SqlDbType.Timestamp);
			sqlDbTypeMap.Add(SqlDataType.TinyInt, SqlDbType.TinyInt);
			sqlDbTypeMap.Add(SqlDataType.Udt, SqlDbType.Udt);
			sqlDbTypeMap.Add(SqlDataType.UniqueIdentifier, SqlDbType.UniqueIdentifier);
			sqlDbTypeMap.Add(SqlDataType.VarBinary, SqlDbType.VarBinary);
			sqlDbTypeMap.Add(SqlDataType.VarChar, SqlDbType.VarChar);
			sqlDbTypeMap.Add(SqlDataType.Xml, SqlDbType.Xml);
			return sqlDbTypeMap;
		}

		internal override void OnExceptionReceived(Exception exception)
		{
			SqlException sqlException = exception as SqlException;
			if (sqlException == null)
			{
				throw exception;
			}

			OnSqlExceptionReceived(sqlException);
		}

		internal void OnSqlExceptionReceived(SqlException sqlException)
		{
			PimodSqlException pimodSqlException = PimodSqlException.Create(sqlException, GetPimodSqlErrors(sqlException.Errors), sqlException.Message);
			OnPimodSqlExceptionReceived(pimodSqlException);
		}

		private static ReadOnlyCollection<PimodSqlError> GetPimodSqlErrors(System.Collections.IEnumerable sqlErrors)
		{
			List<PimodSqlError> errors = new List<PimodSqlError>();
			foreach (SqlError sqlError in sqlErrors)
			{
				errors.Add(new PimodSqlError(sqlError.LineNumber, sqlError.Message, sqlError.Procedure, sqlError.Server, sqlError.Number, sqlError.Class, sqlError.State));
			}
			return errors.AsReadOnly();
		}

		internal void RemoveSqlCommand(SqlCommand command)
		{
			// Do not null out _currentSqlCommand for non-MARS case since this introduces a race that would require a lock.  Instead, just maintain
			// list for MARS case.  
			//
			if (PimodConnectionInfo.EnableMultipleActiveResultSets)
			{
				// Lock must be taken since this is list could be accessed on multiple threads w/ Cancel() call
				//
				lock (_currentSqlCommands)
				{
					_currentSqlCommands.Remove(command);
				}
			}
		}

		protected override void ResetTransactionState()
		{
			PimodExceptions.Validate(_sqlTransaction != null, "BUG IN PIMOD: ResetTransactionState() was called when there was not a transaction");
			_sqlTransaction.Dispose();
			_sqlTransaction = null;
		}

		/// <summary>
		/// Rolls back a transaction
		/// </summary>
		/// <param name="transactionName">The transaction or savepoint name</param>
		/// <remarks>
		/// There should be an active transaction for this operation to succeed
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is not an active transaction</exception>
		protected override void RollbackTransactionInternal(string transactionName)
		{
			// SqlTransaction.Rollback() does not chain into Rollback(transactionName), so handle the two cases separately
			//
			if (transactionName == null)
			{
				_sqlTransaction.Rollback();
			}
			else
			{
				_sqlTransaction.Rollback(transactionName);
			}
		}

		/// <summary>
		/// Saves the transaction
		/// </summary>
		/// <param name="savePointName">Name of the save point.</param>
		/// <remarks>
		/// There should be an active transaction for this operation to succeed
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is not an active transaction</exception>
		protected override void SaveTransactionInternal(string savePointName)
		{
			_sqlTransaction.Save(savePointName);
		}

		/// <summary>
		///  Handles the info message event from sql connection. 
		/// </summary>
		/// <param name="sender">Sender</param>
		/// <param name="e">Event Args</param>
		private void SqlConnectionInfoMessage(object sender, SqlInfoMessageEventArgs e)
		{
			OnPimodSqlErrorsReceived(GetPimodSqlErrors(e.Errors));
		}

		internal override void ToggleInfoMessageEventing(bool enable)
		{
			if (enable)
			{
				_sqlConnection.InfoMessage += SqlConnectionInfoMessage;
			}
			else
			{
				_sqlConnection.InfoMessage -= SqlConnectionInfoMessage;
			}
		}
		#endregion
	}

	internal class PimodOdbcConnection : PimodConnection
	{
		#region Fields

		private static readonly string[] _nativeClients = new[] {
			"SQL Server Native Client",
			"ODBC Driver 18 for SQL Server",
			"ODBC Driver 18 for SQL Server"
		};

		private List<OdbcCommand> _allPreparedCommands;
		private OdbcCommand _currentOdbcCommand;
		private List<OdbcCommand> _currentOdbcCommands;
		private OdbcConnection _odbcConnection;
		private PimodOdbcTransaction _odbcTransaction;
		private bool _useSqlNativeClient;

		private static readonly Dictionary<SqlDataType, OdbcType> _odbcTypeMap = GetOdbcTypeMap();

		#endregion

		#region Constructors

		public PimodOdbcConnection(PimodConnectionInfo pimodConnectionInfo)
			: base(pimodConnectionInfo)
		{
			string odbcConnectionString = CreateOdbcConnectionString(pimodConnectionInfo);
			_odbcConnection = new OdbcConnection(odbcConnectionString);
			_useSqlNativeClient = _nativeClients.Any(client => odbcConnectionString.Contains(client));
			_odbcConnection.ConnectionTimeout = pimodConnectionInfo.ConnectionTimeout;
			_allPreparedCommands = new List<OdbcCommand>();

			// If MARS is enabled, we need to keep a list of commands.  Set that up.  Otherwise, we just need to hang on to the current one (for Close())
			//
			if (pimodConnectionInfo.EnableMultipleActiveResultSets)
			{
				_currentOdbcCommands = new List<OdbcCommand>();
			}
		}

		#endregion

		#region Properties

		/// <summary>
		/// Gets the generic db connection.
		/// </summary>
		/// <value>The db connection.</value>
		protected override IDbConnection DbConnection
		{
			get { return _odbcConnection; }
		}

		/// <summary>
		/// Gets the generic db transaction.
		/// </summary>
		/// <value>The db transaction.</value>
		protected internal override IDbTransaction DbTransaction
		{
			get { return _odbcTransaction; }
		}

		internal OdbcConnection OdbcConnection
		{
			get { return _odbcConnection; }
		}

		internal bool UsesSqlNativeClient
		{
			get { return _useSqlNativeClient; }
		}

		#endregion

		#region Methods

		/// <summary>
		/// Starts a transaction on the current session.
		/// </summary>
		/// <param name="isolationLevel">Isolation level</param>
		/// <param name="transactionName"></param>
		/// <remarks>
		/// There should be only one active transaction per session.
		/// This method will throw an exception if there is already an active transaction
		/// in this session.
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is already an active transaction</exception>
		/// <exception cref="ArgumentException">Occurs if IsolationLevel.Unspecified is passed</exception>
		protected override void BeginTransactionCore(IsolationLevel isolationLevel, string transactionName)
		{
			_odbcTransaction = BeginTransactionInternal(isolationLevel, transactionName);
		}

		protected virtual PimodOdbcTransaction BeginTransactionInternal(IsolationLevel isolationLevel, string transactionName)
		{
			PimodOdbcTransaction transaction = new PimodOdbcTransaction(this.OdbcConnection, isolationLevel);
			try
			{
				transaction.Begin(transactionName);
			}
			catch
			{
				transaction.Dispose();
				throw;
			}
			return transaction;
		}

		public override void Cancel()
		{
			// Note: It is always safe to call SqlCommand.Cancel().  If there is not a current execution, it will be a no-op rather than throwing.  This 
			// function continues on that logic
			//
			if (!PimodConnectionInfo.EnableMultipleActiveResultSets)
			{
				if (_currentOdbcCommand == null)
				{
					return;
				}
				_currentOdbcCommand.Cancel();
			}
			else
			{
				// Lock must be taken since this is list could be accessed on multiple threads between this call and Execute calls
				//
				lock (_currentOdbcCommands)
				{
					foreach (OdbcCommand command in _currentOdbcCommands)
					{
						command.Cancel();
					}
				}
			}
		}

		/// <summary>
		/// Method for underlying PimodConnection implementation to change the database.
		/// </summary>
		/// <param name="databaseName">Name of the database.</param>
		protected override void ChangeDatabaseInternal(string databaseName)
		{
			try
			{
				_odbcConnection.ChangeDatabase(databaseName);
			}
			catch (OdbcException e)
			{
				OnOdbcExceptionReceived(e);
			}
			catch (InvalidOperationException e)
			{
				OnInvalidOperationExceptionReceived(e);
			}
		}

		/// <summary>
		///  Creates an OdbcCommand from the CommandInfo
		/// </summary>
		/// <returns>OdbcCommand equivalent of the command object</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity")]
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:ReviewSqlQueriesForSecurityVulnerabilities", Justification = "Command Info data copied by design", Scope = "method")]
		internal override IDbCommand CreateDbCommand(PimodCommandInfo commandInfo)
		{
			if (commandInfo.IsPrepared)
			{
				IDbCommand dbCommand;
				if (commandInfo.TryGetPreparedCommand(this, out dbCommand))
				{
					return dbCommand;
				}

			}
			OdbcCommand result = new OdbcCommand();
			try
			{
				result.Connection = this.OdbcConnection;
				//DEVNOTE HACK: ODBC does not support named parameters for batch execution.  In order to support queries that were written with parameters, we will embed 
				//the query into a sp_executesql call and pass the ODBC parameters to it with ?s
				//
				if (commandInfo.CommandType == CommandType.Text && commandInfo.Parameters != null && commandInfo.Parameters.Count > 0 && !String.IsNullOrEmpty(commandInfo.Parameters[0].ParameterName))
				{
					StringBuilder parameterDefinitions = new StringBuilder();
					StringBuilder parameterValues = new StringBuilder();
					bool hasReturnValue = false;
					foreach (PimodParameter parameter in commandInfo.Parameters)
					{
						if (parameter.ParameterDirection == ParameterDirection.ReturnValue)
						{
							hasReturnValue = true;
							continue;
						}

						// Add to the parameter definitions and values used within sp_executesql
						//
						if (parameterValues.Length != 0)
						{
							parameterDefinitions.Append(", ");
							parameterValues.Append(", ");
						}

						// Pull the parameter value & definition together
						//
						parameterValues.AppendFormat("{0} = ?", parameter.ParameterName);
						parameterDefinitions.AppendFormat("{0} {1}", parameter.ParameterName, parameter.ParameterType.GetParameterDefinition(parameter.ParameterDirection));
					}

					if (commandInfo.CommandType == CommandType.Text)
					{
						// We need to escape the command text since it is used within a parameter
						//
						string escapedCommandText = commandInfo.CommandText.Replace("'", "''");
						result.CommandText = String.Format("exec {0}sp_executesql N'{1}', N'{2}', {3}", hasReturnValue ? "? = " : String.Empty, escapedCommandText, parameterDefinitions.ToString(), parameterValues.ToString());
					}
					else
					{
						result.CommandText = String.Format("exec {0}{1} {2}", hasReturnValue ? "? = " : String.Empty, commandInfo.CommandText, parameterValues.ToString());
					}
				}
				else if (commandInfo.CommandType == CommandType.StoredProcedure)
				{
					//Use Odbc CALL syntax for direct stored procedure invocation
					//
					bool hasReturnValue = false;
					StringBuilder parameterValues = new StringBuilder();
					foreach (PimodParameter parameter in commandInfo.Parameters)
					{
						if (parameter.ParameterDirection == ParameterDirection.ReturnValue)
						{
							hasReturnValue = true;
							continue;
						}
						parameterValues.Append("?, ");
					}
					if (parameterValues.Length > 0)
					{
						parameterValues.Length -= 2;
					}
					result.CommandText = String.Concat("{ ", hasReturnValue ? "? = " : String.Empty, "CALL ", commandInfo.CommandText, " (", parameterValues.ToString(), ")}");
				}
				else
				{
					result.CommandText = commandInfo.CommandText;
				}

				result.CommandTimeout = commandInfo.CommandTimeout.HasValue ? commandInfo.CommandTimeout.Value : PimodConnectionInfo.DefaultCommandTimeout;
				result.CommandType = commandInfo.CommandType;

				if (commandInfo.Parameters != null)
				{
					foreach (PimodParameter pimodParameter in commandInfo.Parameters)
					{
						OdbcParameter odbcParameter = CreateOdbcParameter(pimodParameter);
						if (commandInfo.IsPrepared)
						{
							pimodParameter.PreparedParameters.Add(odbcParameter);
						}
						if (pimodParameter.ParameterDirection != ParameterDirection.Input)
						{
							if (commandInfo.IsPrepared && pimodParameter.LastExecutedParameter != null)
							{
								//DEVNOTE: We do not support executing a prepared command on multiple connections - 
								//because prepared commands cache the underlying DbCommand, there is no good way to track which was the last
								//command to execute, hence which was the last one we should get the OutputValue from
								//
								throw new NotSupportedException("Cannot execute a Prepared Command with Ouput Parameters on multiple different connections.");
							}
							pimodParameter.LastExecutedParameter = odbcParameter;
						}
						result.Parameters.Add(odbcParameter);
						if (pimodParameter.ParameterName == null && pimodParameter.ParameterName != odbcParameter.ParameterName)
						{
							// Ado.net sets a default parameter name when the parameter is added to the collection.
							// Reset it to null to preserve intended behavior for parameter passing.
							//
							odbcParameter.ParameterName = null;
						}
					}
				}
				if (commandInfo.IsPrepared)
				{
					result.Prepare();
					commandInfo.AddPreparedCommand(this, result);
					// Unlike SqlCommands and OleDbCommands, prepared OdbcCommands need to be disposed 
					// to release native statement handles, so we keep a list of them 
					// and dispose them as part of PimodConnection.Dispose
					//
					_allPreparedCommands.Add(result);
				}
			}
			catch
			{
				result.Dispose();
				throw;
			}
			return result;
		}

		internal static string CreateOdbcConnectionString(PimodConnectionInfo connectionInfo)
		{
			if (connectionInfo.AttachDatabaseFileName != null)
			{
				throw new ArgumentException("AttachDatabaseFileName is not a valid connection property on Odbc connections.", "connectionInfo");
			}

			if (connectionInfo.Authentication.HasValue)
			{
				throw new ArgumentException("Authentication is not a valid connection property on Odbc connections.", "connectionInfo");
			}

			OdbcConnectionStringBuilder connectionBuilder = new OdbcConnectionStringBuilder();
			if (connectionInfo.UseIntegratedSecurity)
			{
				connectionBuilder.Add("trusted_connection", "yes");
			}
			else
			{
				connectionBuilder.Add("Uid", connectionInfo.UserName);
				connectionBuilder.Add("Pwd", connectionInfo.Password);
			}

			if (connectionInfo.EnableMultipleActiveResultSets)
			{
				connectionBuilder.Add("MARS_Connection", "yes");
			}

			if (String.IsNullOrEmpty(connectionInfo.NativeDriver))
			{
				//If no Driver is specified, by default use Sql Native Client.
				//
				connectionBuilder.Driver = "ODBC Driver 18 for SQL Server";
			}
			else
			{
				connectionBuilder.Driver = connectionInfo.NativeDriver;
			}

			if (_nativeClients.Any(client => connectionBuilder.Driver.Contains(client)))
			{
				//OdbcConnections using SQL Native Client require specifying the serverName using the Server attribute,
				//while other drivers (eg, the one used by Project Madison), require Host
				//
				string serverName = String.Format("{0}{1}", connectionInfo.ServerName, connectionInfo.Port.HasValue ? "," + connectionInfo.Port.Value : String.Empty);
				connectionBuilder.Add("Server", serverName);
			}
			else
			{
				connectionBuilder.Add("Host", connectionInfo.ServerName);
				if (connectionInfo.Port.HasValue)
				{
					connectionBuilder.Add("Port", connectionInfo.Port.Value);
				}
			}

			if (!String.IsNullOrEmpty(connectionInfo.ServerSpn))
			{
				connectionBuilder.Add("ServerSPN", connectionInfo.ServerSpn);
			}

			if (!String.IsNullOrEmpty(connectionInfo.DatabaseName))
			{
				connectionBuilder.Add("Database", connectionInfo.DatabaseName);
			}

			if (connectionInfo.UseMultipleSubnetFailover)
			{
				connectionBuilder.Add("MultiSubnetFailover", "yes");
			}

			if (!String.IsNullOrEmpty(connectionInfo.ApplicationName))
			{
				connectionBuilder.Add("APP", connectionInfo.ApplicationName);
			}

			// The ODBC connection string builder doesn't tell us which keys are allowed so we'll use the sqlclient one and check for support there as a proxy.
			//
			SqlConnectionStringBuilder scb = new SqlConnectionStringBuilder();
			if (scb.ContainsKey("ConnectRetryCount"))
			{
				// the sql client connection string supports ConnectRetry, therefore we will support it in odbc so we'll set it
				//
				connectionBuilder["ConnectRetryCount"] = connectionInfo.ConnectRetryCount;
				connectionBuilder["ConnectRetryInterval"] = connectionInfo.ConnectRetryInterval;
			}

			// connectionInfo.UsePooling
			//
			// Reference: https://docs.microsoft.com/en-us/previous-versions/ms810829(v=msdn.10)?redirectedfrom=MSDN
			//
			// Pooling is enabled by default in System.Data.Odbc.OdbcEnvironmentHandle and can only be controlled
			// at the driver level.  Reference: SQL_ATTR.CONNECTION_POOLING
			//

			return connectionBuilder.ToString();
		}

		private static OdbcParameter CreateOdbcParameter(PimodParameter parameter)
		{
			OdbcParameter odbcParameter = CreateOdbcParameterForType(parameter.ParameterType);
			odbcParameter.ParameterName = parameter.ParameterName;
			odbcParameter.Direction = parameter.ParameterDirection;

			// Parameter value is null for output parameters.  Null reference values can also be used to use server default values when
			// the CommandType == CommandType.StoredProcedure
			//
			if (parameter.InputValue != null)
			{
				if (parameter.ParameterType.SystemDataType == SqlDataType.Udt)
				{
					odbcParameter.Value = parameter.InputValue.IsNull ? DBNull.Value : (object)((PimodUdt)parameter.InputValue).ToByteArray();
				}
				else if (MapTypeToString(parameter.ParameterType))
				{
					//Time, DateTime2, DateTimeOffset, Decimal, and Numeric values are sent as strings over Parameters.
					//
					string stringValue = parameter.InputValue.ToString();
					odbcParameter.Value = parameter.InputValue.IsNull ? DBNull.Value : (object)stringValue;
				}
				else
				{
					odbcParameter.Value = parameter.InputValue.IsNull ? DBNull.Value : parameter.InputValue.ToBCLValue();
				}
			}
			return odbcParameter;
		}

		private static OdbcParameter CreateOdbcParameterForType(PimodType pimodType)
		{
			if (!_odbcTypeMap.ContainsKey(pimodType.SystemDataType))
			{
				throw new NotSupportedException(
					string.Format(
						"PimodOdbcConnection does not support {0} SqlDataType. It is not known how to map it to an OdbcType.",
						pimodType.SystemDataType));
			}

			OdbcParameter odbcParameter = new OdbcParameter();
			odbcParameter.OdbcType = _odbcTypeMap[pimodType.SystemDataType];

			odbcParameter.Scale = (byte)pimodType.Scale;
			odbcParameter.Precision = (byte)pimodType.Precision;
			if (pimodType is PimodLengthType)
			{
				odbcParameter.Size = pimodType.Length;
			}
			else if (pimodType.IsLob)
			{
				odbcParameter.Size = -1;
			}
			else if (MapTypeToString(pimodType))
			{
				//For types that are sent as strings, use a fixed-size string large enough to hold all values
				//
				odbcParameter.Size = 64;
			}
			else
			{
				odbcParameter.Size = pimodType.MaxStorageSize;
			}

			return odbcParameter;
		}

		/// <summary>
		/// Disposes and thus closes the connection
		/// </summary>
		protected override void Dispose(bool disposing)
		{
			bool isDisposed = IsDisposed;
			base.Dispose(disposing);

			if (!isDisposed)
			{
				foreach (OdbcCommand odbcCommand in _allPreparedCommands)
				{
					odbcCommand.Dispose();
				}
				_odbcConnection.Dispose();
				_odbcConnection.InfoMessage -= OdbcConnectionInfoMessage;
			}
		}

		protected override int ExecuteNonQueryInternal(PimodCommandInfo commandInfo, bool suppressExecutingEvent = false)
		{
			int rowCount = 0;
			using (OdbcCommand odbcCommand = GetOdbcCommand(commandInfo))
			{
				if (!suppressExecutingEvent)
				{
					// Raise the command now executing event to allow loggers to log the actual command being executed.  Do this after the SqlCommand
					// is created to ensure that any changes to the commandInfo do not have an effect.
					//
					OnCommandNowExecuting(commandInfo);
				}

				Stopwatch s = Stopwatch.StartNew();
				try
				{
					rowCount = odbcCommand.ExecuteNonQuery();
				}
				catch (OdbcException odbcException)
				{
					OnCommandFailed(odbcException, s.Elapsed, commandInfo);

					try
					{
						OnOdbcExceptionReceived(odbcException);
					}
					finally
					{
						CheckForZombieTransaction();
					}
				}
				catch (InvalidOperationException invalidOperationException)
				{
					OnCommandFailed(invalidOperationException, s.Elapsed, commandInfo);

					try
					{
						OnInvalidOperationExceptionReceived(invalidOperationException);
					}
					finally
					{
						CheckForZombieTransaction();
					}
				}
				finally
				{
					RemoveOdbcCommand(odbcCommand);
				}
			}
			return rowCount;
		}

		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "Done to allow for connection abstraction")]
		internal override PimodDataReader ExecuteReaderInternal(PimodCommandInfo commandInfo, PimodCommandContext commandContext, PimodDataReaderBehavior readerBehavior)
		{
			OdbcCommand odbcCommand = null;
			try
			{
				odbcCommand = GetOdbcCommand(commandInfo);
				// Raise the command now executing event to allow loggers to log the actual command being executed.  Do this after the SqlCommand
				// is created to ensure that any changes to the commandInfo do not have an effect.
				//
				OnCommandNowExecuting(commandInfo);

				Stopwatch s = Stopwatch.StartNew();
				try
				{
					// Due to the way that sql variants are read, the command must be read sequentially.  The PDR handles and hides this from user
					//
					OdbcDataReader sqlDataReader = odbcCommand.ExecuteReader(CommandBehavior.SequentialAccess);
					return PimodDataReader.Create(this, odbcCommand, sqlDataReader, commandContext, readerBehavior);
				}
				catch (Exception odbcException)
				{
					// Catch exception to include both OdbcException and the special InvalidOperationException which Odbc throws for closed connections.
					//
					try
					{
						OnCommandFailed(odbcException, s.Elapsed, commandInfo);

						// Raise event, which would throw a PimodSqlException that is created from the sqlException.
						//
						OnExceptionReceived(odbcException);
						RemoveOdbcCommand(odbcCommand);
					}
					catch (PimodSqlException pse)
					{
						if ((readerBehavior & PimodDataReaderBehavior.DoNotDelayException) == PimodDataReaderBehavior.DoNotDelayException)
						{
							throw;
						}
						CheckForZombieTransaction();
						commandContext.Dispose();
						RemoveOdbcCommand(odbcCommand);
						return PimodDataReader.Create(pse, commandContext.Errors);
					}
				}

				return null;
			}
			finally
			{
				// If parameters are bound we don't dispose early or the
				// PimodDataReader does not have parameters to find for
				// output and return values.
				//
				if (odbcCommand != null
					&& (odbcCommand.Parameters == null || odbcCommand.Parameters.Count == 0)
					&& !commandInfo.IsPrepared )
				{
					//If we did not Prepare the OdbcCommand, Dispose it
					//
					odbcCommand.Dispose();
				}
			}
		}



		private OdbcCommand GetOdbcCommand(PimodCommandInfo command)
		{
			OdbcCommand odbcCommand = (OdbcCommand)CreateDbCommand(command);

			//UNDONE:
			//_odbcConnection.FireInfoMessageEventOnUserErrors = command.TreatErrorsAsMessages.HasValue ? command.TreatErrorsAsMessages.Value : PimodConnectionInfo.DefaultTreatErrorsAsMessages;

			if (!PimodConnectionInfo.EnableMultipleActiveResultSets)
			{
				_currentOdbcCommand = odbcCommand;
			}
			else
			{
				// Lock must be taken since this is list could be accessed on multiple threads w/ Cancel() call
				//
				lock (_currentOdbcCommands)
				{
					_currentOdbcCommands.Add(odbcCommand);
				}
			}
			return odbcCommand;
		}

		private static Dictionary<SqlDataType, OdbcType> GetOdbcTypeMap()
		{
			Dictionary<SqlDataType, OdbcType> dbTypeMap = new Dictionary<SqlDataType, OdbcType>();
			dbTypeMap.Add(SqlDataType.BigInt, OdbcType.BigInt);
			dbTypeMap.Add(SqlDataType.Binary, OdbcType.Binary);
			dbTypeMap.Add(SqlDataType.Bit, OdbcType.Bit);
			dbTypeMap.Add(SqlDataType.Char, OdbcType.Char);
			dbTypeMap.Add(SqlDataType.Date, OdbcType.Date);
			dbTypeMap.Add(SqlDataType.DateTime, OdbcType.DateTime);
			dbTypeMap.Add(SqlDataType.DateTimeOffset, OdbcType.VarChar);
			dbTypeMap.Add(SqlDataType.DateTime2, OdbcType.VarChar);
			dbTypeMap.Add(SqlDataType.Decimal, OdbcType.VarChar);
			dbTypeMap.Add(SqlDataType.Float, OdbcType.Double);
			dbTypeMap.Add(SqlDataType.Image, OdbcType.Image);
			dbTypeMap.Add(SqlDataType.Int, OdbcType.Int);
			dbTypeMap.Add(SqlDataType.Money, OdbcType.Decimal);
			dbTypeMap.Add(SqlDataType.NChar, OdbcType.NChar);
			dbTypeMap.Add(SqlDataType.NText, OdbcType.NText);
			dbTypeMap.Add(SqlDataType.Numeric, OdbcType.VarChar);
			dbTypeMap.Add(SqlDataType.NVarChar, OdbcType.NVarChar);
			dbTypeMap.Add(SqlDataType.Real, OdbcType.Real);
			dbTypeMap.Add(SqlDataType.SmallDateTime, OdbcType.SmallDateTime);
			dbTypeMap.Add(SqlDataType.SmallInt, OdbcType.SmallInt);
			dbTypeMap.Add(SqlDataType.SmallMoney, OdbcType.Decimal);
			dbTypeMap.Add(SqlDataType.SqlVariant, OdbcType.Binary);
			dbTypeMap.Add(SqlDataType.Text, OdbcType.Text);
			dbTypeMap.Add(SqlDataType.Time, OdbcType.VarChar);
			dbTypeMap.Add(SqlDataType.Timestamp, OdbcType.Timestamp);
			dbTypeMap.Add(SqlDataType.TinyInt, OdbcType.TinyInt);
			dbTypeMap.Add(SqlDataType.Udt, OdbcType.VarBinary);
			dbTypeMap.Add(SqlDataType.UniqueIdentifier, OdbcType.UniqueIdentifier);
			dbTypeMap.Add(SqlDataType.VarBinary, OdbcType.VarBinary);
			dbTypeMap.Add(SqlDataType.VarChar, OdbcType.VarChar);
			dbTypeMap.Add(SqlDataType.Xml, OdbcType.NText);
			return dbTypeMap;
		}

		private static ReadOnlyCollection<PimodSqlError> GetPimodSqlErrors(System.Collections.IEnumerable odbcErrors)
		{
			List<PimodSqlError> errors = new List<PimodSqlError>();
			foreach (OdbcError odbcError in odbcErrors)
			{
				string message = StripOdbcDriverHeader(odbcError.Message);

				// For a query timeout the native error is 0 and when bubbled
				// in the PimodSqlError as 0 is strange.  Map to -2 to match
				// the .NET SqlClient timeout error number.
				//
				int nativeError = odbcError.NativeError;
				if (nativeError == 0)
				{
					nativeError = -2;
				}

				errors.Add(new PimodSqlError(0, message, null, odbcError.Source, nativeError, 0, 0, odbcError.SQLState));
			}
			return errors.AsReadOnly();
		}

		/// <summary>
		/// Determines if we will send a type as a string value over the wire, to avoid rounding/truncation/unsupported errors through System.Data.Odbc
		/// </summary>
		private static bool MapTypeToString(PimodType pimodType)
		{
			return pimodType.SystemDataType == SqlDataType.Time || pimodType.SystemDataType == SqlDataType.DateTimeOffset || pimodType.SystemDataType == SqlDataType.DateTime2 || pimodType.SystemDataType == SqlDataType.Decimal || pimodType.SystemDataType == SqlDataType.Numeric;
		}

		/// <summary>
		///  Handles the info message event from odbc connection. 
		/// </summary>
		/// <param name="sender">Sender</param>
		/// <param name="e">Event Args</param>
		private void OdbcConnectionInfoMessage(object sender, OdbcInfoMessageEventArgs e)
		{
			OnPimodSqlErrorsReceived(GetPimodSqlErrors(e.Errors));
		}

		internal override void OnExceptionReceived(Exception exception)
		{
			OdbcException odbcException = exception as OdbcException;
			if (odbcException != null)
			{
				OnOdbcExceptionReceived(odbcException);
				return;
			}

			InvalidOperationException invalidOperationException = exception as InvalidOperationException;
			if (invalidOperationException != null)
			{
				OnInvalidOperationExceptionReceived(invalidOperationException);
				return;
			}

			throw exception;
		}

		/// <summary>
		/// For Odbc, special case Invalid Operation Exceptions which are thrown instead of OdbcExceptions when the connection is closed.
		/// Manually map the exception to error 233 to allow rethrowing as a PimodSqlException to allow tests to consistently handle PimodSqlExceptions.
		/// 
		/// We assume all InvalidOperationExceptions from odbc are due to connection being closed as the other cases where it is thrown are either specific to Oracle
		/// or user bugs for not setting CommandText or the Connection properties of the OdbcCommand that should never occur with Pimod.
		/// We can't filter based on the string as it is localized.
		/// </summary>
		internal void OnInvalidOperationExceptionReceived(InvalidOperationException invalidOperationException)
		{
			string message = StripOdbcDriverHeader(invalidOperationException.Message);

			// For ODBC connection failures we must set severity to 20 or greater so locations such
			// as Microsoft.SqlServer.Test.Pimod.PimodDataReader.Create can exit the loop.  If we do not
			// set the severity the while loop sees error 233, Sev=0 and continues to loop.  This builds
			// up a massive exception list, causing memory issues with no forward progress.
			//
			// Microsoft.SqlServer.Test.Pimod.PimodOdbcConnection.OnInvalidOperationExceptionReceived
			// Microsoft.SqlServer.Test.Pimod.PimodDataReader + PimodDataReaderFromOdbcDataReader.NextResult
			// ...
			// System_Data!System::Data::Odbc::OdbcDataReader::NextResult
			// Microsoft.SqlServer.Test.Pimod.PimodDataReader + PimodDataReaderFromOdbcDataReader.NextResult
			// Microsoft.SqlServer.Test.Pimod.PimodDataReader + PimodDataReaderBufferedInMemory.Create
			//
			ReadOnlyCollection<PimodSqlError> pimodSqlErrors = (new List<PimodSqlError>() { new PimodSqlError(0, message, null, null, 233, 20, 0, null) }).AsReadOnly();
			PimodSqlException pimodSqlException = PimodSqlException.Create(invalidOperationException, pimodSqlErrors, message);
			OnPimodSqlExceptionReceived(pimodSqlException);
		}

		internal void OnOdbcExceptionReceived(OdbcException odbcException)
		{
			PimodSqlException pimodSqlException = PimodSqlException.Create(odbcException, GetPimodSqlErrors(odbcException.Errors), StripOdbcDriverHeader(odbcException.Message));
			OnPimodSqlExceptionReceived(pimodSqlException);
		}

		internal void RemoveOdbcCommand(OdbcCommand command)
		{
			// Do not null out _currentSqlCommand for non-MARS case since this introduces a race that would require a lock.  Instead, just maintain
			// list for MARS case.  
			//
			if (PimodConnectionInfo.EnableMultipleActiveResultSets)
			{
				// Lock must be taken since this is list could be accessed on multiple threads w/ Cancel() call
				//
				lock (_currentOdbcCommands)
				{
					_currentOdbcCommands.Remove(command);
				}
			}
		}

		protected override void ResetTransactionState()
		{
			PimodExceptions.Validate(_odbcTransaction != null, "BUG IN PIMOD: ResetTransactionState() was called when there was not a transaction");
			_odbcTransaction.Dispose();
			_odbcTransaction = null;
		}

		protected override void RollbackTransactionInternal(string transactionName)
		{
			_odbcTransaction.Rollback();
		}

		internal static string StripOdbcDriverHeader(string message)
		{
			//We want to strip out the Header to exceptions so just the message text is returned
			//Examples:
			//"ERROR [HYT00] [Microsoft][SQL Server Native Client 10.0]"
			//"[Microsoft][SQL Server Native Client 10.0][SQL Server]"
			//"ERROR [22012] [Microsoft][SQL Server Native Client 10.0][SQL Server]"
			//"ERROR [42000] [Microsoft][SQL Server Native Client 10.0][SQL Server]"
			// "[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]"
			// "ERROR [22012] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]"
			// "ERROR [42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]"
			//
			Regex odbcHeader = new Regex(@"^(ERROR \[[\d\w]*\])?\s*\[Microsoft\](\[SQL Server Native Client \d+\.\d+\]|\[ODBC Driver 18 for SQL Server\])(\[SQL Server\])?", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Multiline);
			return odbcHeader.Replace(message, String.Empty);
		}

		internal override void ToggleInfoMessageEventing(bool enable)
		{
			if (enable)
			{
				_odbcConnection.InfoMessage += OdbcConnectionInfoMessage;
			}
			else
			{
				_odbcConnection.InfoMessage -= OdbcConnectionInfoMessage;
			}
		}

		#endregion
	}

	internal class PimodOleDbConnection : PimodConnection
	{
		#region Fields

		private OleDbConnection _oleDbConnection;
		private PimodOleDbTransaction _oleDbTransaction;
		private OleDbCommand _currentOleDbCommand;
		private List<OleDbCommand> _currentOleDbCommands;
		private static readonly Dictionary<SqlDataType, OleDbType> _oleDbTypeMap = GetOleDbTypeMap();
		private bool _useSqlNativeClient;

		#endregion

		#region Constructors

		public PimodOleDbConnection(PimodConnectionInfo pimodConnectionInfo)
			: base(pimodConnectionInfo)
		{
			string oleDbConnectionString = CreateOleDbConnectionString(pimodConnectionInfo);
			_oleDbConnection = new OleDbConnection(oleDbConnectionString);
			_useSqlNativeClient = oleDbConnectionString.Contains("SQLNCLI");

			// If MARS is enabled, we need to keep a list of commands.  Set that up.  Otherwise, we just need to hang on to the current one (for Close())
			//
			if (pimodConnectionInfo.EnableMultipleActiveResultSets)
			{
				_currentOleDbCommands = new List<OleDbCommand>();
			}
		}

		#endregion

		#region Properties

		/// <summary>
		/// Gets the generic db connection.
		/// </summary>
		/// <value>The db connection.</value>
		protected override IDbConnection DbConnection
		{
			get { return _oleDbConnection; }
		}

		/// <summary>
		/// Gets the generic db transaction.
		/// </summary>
		/// <value>The db transaction.</value>
		protected internal override IDbTransaction DbTransaction
		{
			get { return _oleDbTransaction; }
		}

		internal OleDbConnection OleDbConnection
		{
			get { return _oleDbConnection; }
		}

		internal bool UsesSqlNativeClient
		{
			get { return _useSqlNativeClient; }
		}

		#endregion

		#region Methods

		/// <summary>
		/// Starts a transaction on the current session.
		/// </summary>
		/// <param name="isolationLevel">Isolation level</param>
		/// <param name="transactionName"></param>
		/// <remarks>
		/// There should be only one active transaction per session.
		/// This method will throw an exception if there is already an active transaction
		/// in this session.
		/// </remarks>
		/// <exception cref="InvalidOperationException">Occurs when there is already an active transaction</exception>
		/// <exception cref="ArgumentException">Occurs if IsolationLevel.Unspecified is passed</exception>
		protected override void BeginTransactionCore(IsolationLevel isolationLevel, string transactionName)
		{
			_oleDbTransaction = BeginTransactionInternal(isolationLevel, transactionName);
		}

		protected virtual PimodOleDbTransaction BeginTransactionInternal(IsolationLevel isolationLevel, string transactionName)
		{
			PimodOleDbTransaction transaction = new PimodOleDbTransaction(this.OleDbConnection, isolationLevel);
			try
			{
				transaction.Begin(transactionName);
			}
			catch
			{
				transaction.Dispose();
				throw;
			}
			return transaction;
		}

		public override void Cancel()
		{
			// Note: It is always safe to call SqlCommand.Cancel().  If there is not a current execution, it will be a no-op rather than throwing.  This 
			// function continues on that logic
			//
			if (!PimodConnectionInfo.EnableMultipleActiveResultSets)
			{
				if (_currentOleDbCommand == null)
				{
					return;
				}

				_currentOleDbCommand.Cancel();
			}
			else
			{
				// Lock must be taken since this is list could be accessed on multiple threads between this call and Execute calls
				//
				lock (_currentOleDbCommands)
				{
					foreach (OleDbCommand command in _currentOleDbCommands)
					{
						command.Cancel();
					}
				}
			}
		}

		/// <summary>
		/// Method for underlying PimodConnection implementation to change the database.
		/// </summary>
		/// <param name="databaseName">Name of the database.</param>
		protected override void ChangeDatabaseInternal(string databaseName)
		{
			try
			{
				_oleDbConnection.ChangeDatabase(databaseName);
			}
			catch (OleDbException e)
			{
				OnOleDbExceptionReceived(e);
			}
		}

		/// <summary>
		///  Creates an OleDbCommand from the CommandInfo
		/// </summary>
		/// <returns>OleDbCommand equivalent of the command object</returns>
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity")]
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:ReviewSqlQueriesForSecurityVulnerabilities", Justification = "Command Info data copied by design", Scope = "method")]
		internal override IDbCommand CreateDbCommand(PimodCommandInfo commandInfo)
		{
			if (commandInfo.IsPrepared)
			{
				IDbCommand dbCommand;
				if (commandInfo.TryGetPreparedCommand(this, out dbCommand))
				{
					return dbCommand;
				}

			}
			OleDbCommand result = new OleDbCommand();
			try
			{
				result.Connection = this.OleDbConnection;
				//DEVNOTE HACK: OleDb does not support named parameters.  In order to support queries that were written with parameters, we will embed 
				//the query into a sp_executesql call and pass the OleDb parameters to it with ?s
				//
				if (commandInfo.CommandType == CommandType.Text && commandInfo.Parameters != null && commandInfo.Parameters.Count > 0 && !String.IsNullOrEmpty(commandInfo.Parameters[0].ParameterName))
				{
					bool hasReturnValue = false;
					StringBuilder parameterDefinitions = new StringBuilder();
					StringBuilder parameterValues = new StringBuilder();
					foreach (PimodParameter parameter in commandInfo.Parameters)
					{
						if (parameter.ParameterDirection == ParameterDirection.ReturnValue)
						{
							hasReturnValue = true;
							continue;
						}

						// Add to the parameter definitions and values used within sp_executesql
						//
						if (parameterValues.Length != 0)
						{
							parameterDefinitions.Append(", ");
							parameterValues.Append(", ");
						}

						// Pull the parameter value & definition together
						//
						parameterValues.AppendFormat("{0} = ?{1}", parameter.ParameterName, (parameter.ParameterDirection == ParameterDirection.InputOutput || parameter.ParameterDirection == ParameterDirection.Output) ? " OUTPUT" : String.Empty);
						parameterDefinitions.AppendFormat("{0} {1}", parameter.ParameterName, parameter.ParameterType.GetParameterDefinition(parameter.ParameterDirection));
					}

					if (commandInfo.CommandType == CommandType.Text)
					{
						// We need to escape the command text since it is used within a parameter
						//
						string escapedCommandText = commandInfo.CommandText.Replace("'", "''");
						result.CommandText = String.Format("exec {0}sp_executesql N'{1}', N'{2}', {3}", hasReturnValue ? "? = " : String.Empty, escapedCommandText, parameterDefinitions.ToString(), parameterValues.ToString());
					}
					else
					{
						result.CommandText = String.Format("exec {0}{1} {2}", hasReturnValue ? "? = " : String.Empty, commandInfo.CommandText, parameterValues.ToString());
					}
				}
				else
				{
					result.CommandText = commandInfo.CommandText;
				}

				result.CommandTimeout = commandInfo.CommandTimeout.HasValue ? commandInfo.CommandTimeout.Value : PimodConnectionInfo.DefaultCommandTimeout;
				result.CommandType = commandInfo.CommandType;

				if (commandInfo.Parameters != null)
				{
					foreach (PimodParameter pimodParameter in commandInfo.Parameters)
					{
						OleDbParameter oleDbParameter = CreateOleDbParameter(pimodParameter);
						if (commandInfo.IsPrepared)
						{
							pimodParameter.PreparedParameters.Add(oleDbParameter);
						}
						if (pimodParameter.ParameterDirection != ParameterDirection.Input)
						{
							if (commandInfo.IsPrepared && pimodParameter.LastExecutedParameter != null)
							{
								//DEVNOTE: We do not support executing a prepared command on multiple connections - 
								//because prepared commands cache the underlying DbCommand, there is no good way to track which was the last
								//command to execute, hence which was the last one we should get the OutputValue from
								//
								throw new NotSupportedException("Cannot execute a Prepared Command with Ouput Parameters on multiple different connections.");
							}
							pimodParameter.LastExecutedParameter = oleDbParameter;
						}
						result.Parameters.Add(oleDbParameter);
						if (pimodParameter.ParameterName == null && pimodParameter.ParameterName != oleDbParameter.ParameterName)
						{
							// Ado.net sets a default parameter name when the parameter is added to the collection.
							// Reset it to null to preserve intended behavior for parameter passing.
							//
							oleDbParameter.ParameterName = null;
						}

					}
				}
				if (commandInfo.IsPrepared)
				{
					result.Prepare();
					commandInfo.AddPreparedCommand(this, result);
				}
			}
			catch
			{
				result.Dispose();
				throw;
			}
			return result;
		}

		/// <summary>
		///  Creates an OLE DB connection string from the given PimodConnectionInfo.
		/// </summary>
		/// <returns>OLE DB connection string.</returns>
		public static string CreateOleDbConnectionString(PimodConnectionInfo connectionInfo)
		{
			if (connectionInfo.AttachDatabaseFileName != null)
			{
				throw new ArgumentException("AttachDatabaseFileName is not a valid connection property on OleDb connections.", "connectionInfo");
			}

			if (connectionInfo.Authentication.HasValue)
			{
				throw new ArgumentException("Authentication is not a valid connection property on OleDb connections.", "connectionInfo");
			}

			OleDbConnectionStringBuilder connectionBuilder = new OleDbConnectionStringBuilder();

			// All services except pooling : "OLE DB Services = -2;"
			// Reference: https://docs.microsoft.com/en-us/previous-versions/ms810829(v=msdn.10)?redirectedfrom=MSDN
			//
			if (!connectionInfo.UsePooling)
			{
				connectionBuilder.OleDbServices = -2;
			}
			if (connectionInfo.MaxPoolSize.HasValue)
			{
				connectionBuilder.Add("Max Pool Size", connectionInfo.MaxPoolSize.Value);
			}
			if (connectionInfo.UseIntegratedSecurity)
			{
				if (connectionBuilder.Provider.Contains("Data Warehouse"))
				{
					//OleDbConnections using the Madison Data Warehouse driver require specifying integrated security using trusted_connection
					//while most other drivers accept Integrated Security
					//
					connectionBuilder.Add("trusted_connection", "yes");
				}
				else
				{
					connectionBuilder.Add("Integrated Security", "SSPI");
				}

			}
			else
			{
				connectionBuilder.Add("Uid", connectionInfo.UserName);
				connectionBuilder.Add("Pwd", connectionInfo.Password);
			}

			if (connectionInfo.EnableMultipleActiveResultSets)
			{
				connectionBuilder.Add("MARS_Connection", "yes");
			}

			if (String.IsNullOrEmpty(connectionInfo.NativeDriver))
			{
				// Use the newer MSOLEDBSQL provider if it is installed on the machine
				//
				if (CheckIfOleDbProviderIsInstalled(MSOLEDBSQLProviderName, connectionInfo))
				{
					connectionBuilder.Provider = MSOLEDBSQLProviderName;
				}
				else
				{
					connectionBuilder.Provider = SQLNCLI11ProviderName;
				}
			}
			else
			{
				connectionBuilder.Provider = connectionInfo.NativeDriver;
			}

			if (connectionBuilder.Provider.Contains("Data Warehouse"))
			{
				//OleDbConnections using the Madison Data Warehouse driver require specifying the serverName using the Host attribute,
				//while most other drivers use Data Source
				//
				connectionBuilder.Add("Host", connectionInfo.ServerName);
				if (connectionInfo.Port.HasValue)
				{
					connectionBuilder.Add("Port", connectionInfo.Port.Value);
				}
			}
			else
			{
				string serverName = String.Format("{0}{1}", connectionInfo.ServerName, connectionInfo.Port.HasValue ? "," + connectionInfo.Port.Value : String.Empty);
				connectionBuilder.Add("Data Source", serverName);
			}
			if (!String.IsNullOrEmpty(connectionInfo.ServerSpn))
			{
				connectionBuilder.Add("ServerSPN", connectionInfo.ServerSpn);
			}

			if (!String.IsNullOrEmpty(connectionInfo.DatabaseName))
			{
				connectionBuilder.Add("Database", connectionInfo.DatabaseName);
			}

			// We would also need to add Multiple subnet failover property support in future when sql client will support it. 
			//

			connectionBuilder.Add("Connect Timeout", connectionInfo.ConnectionTimeout);

			// Set the application name.
			//
			if (!string.IsNullOrEmpty(connectionInfo.ApplicationName))
			{
				connectionBuilder.Add("Application Name", connectionInfo.ApplicationName);
			}

			return connectionBuilder.ToString();
		}

		private static OleDbParameter CreateOleDbParameter(PimodParameter parameter)
		{
			OleDbParameter oleDbParameter = CreateOleDbParameterForType(parameter.ParameterType);
			oleDbParameter.ParameterName = parameter.ParameterName;
			oleDbParameter.Direction = parameter.ParameterDirection;

			// Parameter value is null for output parameters.  Null reference values can also be used to use server default values when
			// the CommandType == CommandType.StoredProcedure
			//
			if (parameter.InputValue != null)
			{
				if (parameter.ParameterType.SystemDataType == SqlDataType.Udt)
				{
					oleDbParameter.Value = parameter.InputValue.IsNull ? DBNull.Value : (object)((PimodUdt)parameter.InputValue).ToByteArray();
				}
				else if (MapTypeToString(parameter.ParameterType))
				{
					// Time, DateTime2, DateTimeOffset, Decimal, and Numeric values are sent as strings over Parameters.
					//
					string stringValue = parameter.InputValue.ToString();
					oleDbParameter.Value = parameter.InputValue.IsNull ? DBNull.Value : (object)stringValue;
				}
				else
				{
					oleDbParameter.Value = parameter.InputValue.IsNull ? DBNull.Value : parameter.InputValue.ToBCLValue();
				}
			}
			return oleDbParameter;
		}

		private static OleDbParameter CreateOleDbParameterForType(PimodType pimodType)
		{
			if (!_oleDbTypeMap.ContainsKey(pimodType.SystemDataType))
			{
				throw new NotSupportedException(
					string.Format(
						"PimodOleDbConnection does not support {0} SqlDataType. It is not known how to map it to an OleDbType.",
						pimodType.SystemDataType));
			}

			OleDbParameter oleDbParameter = new OleDbParameter();
			oleDbParameter.OleDbType = _oleDbTypeMap[pimodType.SystemDataType];

			oleDbParameter.Scale = (byte)pimodType.Scale;
			oleDbParameter.Precision = (byte)pimodType.Precision;
			if (pimodType is PimodLengthType)
			{
				oleDbParameter.Size = pimodType.Length;
			}
			else if (pimodType.IsLob)
			{
				oleDbParameter.Size = -1;
			}
			else if (MapTypeToString(pimodType))
			{
				//For types that are sent as strings, use a fixed-size string large enough to hold all values
				//
				oleDbParameter.Size = 64;
			}
			else
			{
				oleDbParameter.Size = pimodType.MaxStorageSize;
			}

			return oleDbParameter;
		}

		/// <summary>
		/// Disposes and thus closes the connection
		/// </summary>
		protected override void Dispose(bool disposing)
		{
			bool isDisposed = IsDisposed;
			base.Dispose(disposing);

			if (!isDisposed)
			{
				_oleDbConnection.Dispose();
				_oleDbConnection.InfoMessage -= OleDbConnectionInfoMessage;
			}
		}

		protected override int ExecuteNonQueryInternal(PimodCommandInfo commandInfo, bool suppressExecutingEvent = false)
		{
			int rowCount = 0;
			using (OleDbCommand oleDbCommand = GetOleDbCommand(commandInfo))
			{
				if (!suppressExecutingEvent)
				{
					// Raise the command now executing event to allow loggers to log the actual command being executed.  Do this after the SqlCommand
					// is created to ensure that any changes to the commandInfo do not have an effect.
					//
					OnCommandNowExecuting(commandInfo);
				}

				Stopwatch s = Stopwatch.StartNew();
				try
				{
					rowCount = oleDbCommand.ExecuteNonQuery();
				}
				catch (OleDbException oleDbException)
				{
					OnCommandFailed(oleDbException, s.Elapsed, commandInfo);

					try
					{
						OnOleDbExceptionReceived(oleDbException);
					}
					finally
					{
						CheckForZombieTransaction();
					}
				}
				finally
				{
					RemoveOleDbCommand(oleDbCommand);
				}
			}
			return rowCount;
		}

		internal override PimodDataReader ExecuteReaderInternal(PimodCommandInfo commandInfo, PimodCommandContext commandContext, PimodDataReaderBehavior readerBehavior)
		{
			OleDbCommand oleDbCommand = null;
			try
			{
				oleDbCommand = GetOleDbCommand(commandInfo);
				// Raise the command now executing event to allow loggers to log the actual command being executed.  Do this after the SqlCommand
				// is created to ensure that any changes to the commandInfo do not have an effect.
				//
				OnCommandNowExecuting(commandInfo);

				Stopwatch s = Stopwatch.StartNew();
				try
				{
					// Due to the way that sql variants are read, the command must be read sequentially.  The PDR handles and hides this from user
					//
					OleDbDataReader sqlDataReader = oleDbCommand.ExecuteReader(CommandBehavior.SequentialAccess);
					return PimodDataReader.Create(this, oleDbCommand, sqlDataReader, commandContext, readerBehavior);
				}
				catch (OleDbException oleDbException)
				{
					try
					{
						OnCommandFailed(oleDbException, s.Elapsed, commandInfo);

						// Raise event, which would throw a PimodSqlException that is created from the sqlException.
						//
						OnOleDbExceptionReceived(oleDbException);
						RemoveOleDbCommand(oleDbCommand);
					}
					catch (PimodSqlException pse)
					{
						if ((readerBehavior & PimodDataReaderBehavior.DoNotDelayException) == PimodDataReaderBehavior.DoNotDelayException)
						{
							throw;
						}
						CheckForZombieTransaction();
						commandContext.Dispose();
						RemoveOleDbCommand(oleDbCommand);
						return PimodDataReader.Create(pse, commandContext.Errors);
					}
				}

				return null;
			}
			finally
			{
				if (oleDbCommand != null
					&& (oleDbCommand.Parameters == null || oleDbCommand.Parameters.Count == 0)
					&& !commandInfo.IsPrepared)
				{
					//If we did not Prepare the OleDbCommand, Dispose it
					//
					oleDbCommand.Dispose();
				}
			}
		}

		private OleDbCommand GetOleDbCommand(PimodCommandInfo command)
		{
			OleDbCommand oleDbCommand = (OleDbCommand)CreateDbCommand(command);

			//UNDONE:
			//_oleDbConnection.FireInfoMessageEventOnUserErrors = command.TreatErrorsAsMessages.HasValue ? command.TreatErrorsAsMessages.Value : PimodConnectionInfo.DefaultTreatErrorsAsMessages;

			if (!PimodConnectionInfo.EnableMultipleActiveResultSets)
			{
				_currentOleDbCommand = oleDbCommand;
			}
			else
			{
				// Lock must be taken since this is list could be accessed on multiple threads w/ Cancel() call
				//
				lock (_currentOleDbCommands)
				{
					_currentOleDbCommands.Add(oleDbCommand);
				}
			}
			return oleDbCommand;
		}

		private static Dictionary<SqlDataType, OleDbType> GetOleDbTypeMap()
		{
			Dictionary<SqlDataType, OleDbType> dbTypeMap = new Dictionary<SqlDataType, OleDbType>();
			dbTypeMap.Add(SqlDataType.BigInt, OleDbType.BigInt);
			dbTypeMap.Add(SqlDataType.Binary, OleDbType.Binary);
			dbTypeMap.Add(SqlDataType.Bit, OleDbType.Boolean);
			dbTypeMap.Add(SqlDataType.Char, OleDbType.Char);
			dbTypeMap.Add(SqlDataType.Date, OleDbType.DBDate);
			dbTypeMap.Add(SqlDataType.DateTime, OleDbType.DBTimeStamp);
			dbTypeMap.Add(SqlDataType.DateTimeOffset, OleDbType.VarChar);
			dbTypeMap.Add(SqlDataType.DateTime2, OleDbType.VarChar);
			dbTypeMap.Add(SqlDataType.Decimal, OleDbType.VarChar);
			dbTypeMap.Add(SqlDataType.Float, OleDbType.Double);
			dbTypeMap.Add(SqlDataType.Image, OleDbType.LongVarBinary);
			dbTypeMap.Add(SqlDataType.Int, OleDbType.Integer);
			dbTypeMap.Add(SqlDataType.Money, OleDbType.Decimal);
			dbTypeMap.Add(SqlDataType.NChar, OleDbType.WChar);
			dbTypeMap.Add(SqlDataType.NText, OleDbType.LongVarWChar);
			dbTypeMap.Add(SqlDataType.Numeric, OleDbType.VarChar);
			dbTypeMap.Add(SqlDataType.NVarChar, OleDbType.VarWChar);
			dbTypeMap.Add(SqlDataType.Real, OleDbType.Single);
			dbTypeMap.Add(SqlDataType.SmallDateTime, OleDbType.DBTimeStamp);
			dbTypeMap.Add(SqlDataType.SmallInt, OleDbType.SmallInt);
			dbTypeMap.Add(SqlDataType.SmallMoney, OleDbType.Decimal);
			dbTypeMap.Add(SqlDataType.SqlVariant, OleDbType.Binary);
			dbTypeMap.Add(SqlDataType.Text, OleDbType.LongVarChar);
			dbTypeMap.Add(SqlDataType.Time, OleDbType.VarChar);
			dbTypeMap.Add(SqlDataType.Timestamp, OleDbType.Binary);
			dbTypeMap.Add(SqlDataType.TinyInt, OleDbType.UnsignedTinyInt);
			dbTypeMap.Add(SqlDataType.Udt, OleDbType.VarBinary);
			dbTypeMap.Add(SqlDataType.UniqueIdentifier, OleDbType.Guid);
			dbTypeMap.Add(SqlDataType.VarBinary, OleDbType.VarBinary);
			dbTypeMap.Add(SqlDataType.VarChar, OleDbType.VarChar);
			dbTypeMap.Add(SqlDataType.Xml, OleDbType.VarWChar);
			return dbTypeMap;
		}

		private static ReadOnlyCollection<PimodSqlError> GetPimodSqlErrors(System.Collections.IEnumerable oleDbErrors)
		{
			List<PimodSqlError> errors = new List<PimodSqlError>();
			foreach (OleDbError oleDbError in oleDbErrors)
			{
				// For a query timeout the native error is 0 and when bubbled
				// in the PimodSqlError as 0 is strange.  Map to -2 to match
				// the .NET SqlClient timeout error number.
				//
				int nativeError = oleDbError.NativeError;
				if (nativeError == 0)
				{
					nativeError = -2;
				}

				errors.Add(new PimodSqlError(0, oleDbError.Message, null, oleDbError.Source, nativeError, 0, 0, oleDbError.SQLState));
			}

			// OLEDB can throw E_FAIL and other errors that have no error collection.  Callers catching PiModSqlException
			// are not expecting this and call ex.ErrorNumber which throws.  To avoid strange crashes if the collection
			// is empty we add a dummy message and native error code to provide some context without crashing the test.
			//
			if (errors.Count == 0)
			{
				errors.Add(new PimodSqlError(
											0,					// Line
											"No error messages returned from provider", 
											null,				// Procedure
											"OLEDB Provider",	// Server
											-3,					// ErrorNumber
											0,					// Severity
											0,					// State
											String.Empty));		// SQL State
            }

			return errors.AsReadOnly();
		}

		/// <summary>
		/// Determines if we will send a type as a string value over the wire, to avoid rounding/truncation/unsupported errors through System.Data.OleDb
		/// </summary>
		private static bool MapTypeToString(PimodType pimodType)
		{
			return pimodType.SystemDataType == SqlDataType.Time || pimodType.SystemDataType == SqlDataType.DateTimeOffset || pimodType.SystemDataType == SqlDataType.DateTime2 || pimodType.SystemDataType == SqlDataType.Decimal || pimodType.SystemDataType == SqlDataType.Numeric;
		}

		/// <summary>
		///  Handles the info message event from oleDb connection. 
		/// </summary>
		/// <param name="sender">Sender</param>
		/// <param name="e">Event Args</param>
		private void OleDbConnectionInfoMessage(object sender, OleDbInfoMessageEventArgs e)
		{
			OnPimodSqlErrorsReceived(GetPimodSqlErrors(e.Errors));
		}

		internal override void OnExceptionReceived(Exception exception)
		{
			OleDbException oleDbException = exception as OleDbException;
			if (oleDbException == null)
			{
				throw exception;
			}

			OnOleDbExceptionReceived(oleDbException);
		}

		internal void OnOleDbExceptionReceived(OleDbException oleDbException)
		{
			PimodSqlException pimodSqlException = PimodSqlException.Create(oleDbException, GetPimodSqlErrors(oleDbException.Errors), oleDbException.Message);
			OnPimodSqlExceptionReceived(pimodSqlException);
		}

		internal void RemoveOleDbCommand(OleDbCommand command)
		{
			// Do not null out _currentSqlCommand for non-MARS case since this introduces a race that would require a lock.  Instead, just maintain
			// list for MARS case.  
			//
			if (PimodConnectionInfo.EnableMultipleActiveResultSets)
			{
				// Lock must be taken since this is list could be accessed on multiple threads w/ Cancel() call
				//
				lock (_currentOleDbCommands)
				{
					_currentOleDbCommands.Remove(command);
				}
			}
		}

		protected override void ResetTransactionState()
		{
			PimodExceptions.Validate(_oleDbTransaction != null, "BUG IN PIMOD: ResetTransactionState() was called when there was not a transaction");
			_oleDbTransaction.Dispose();
			_oleDbTransaction = null;
		}

		protected override void RollbackTransactionInternal(string transactionName)
		{
			_oleDbTransaction.Rollback();
		}

		internal override void ToggleInfoMessageEventing(bool enable)
		{
			if (enable)
			{
				_oleDbConnection.InfoMessage += OleDbConnectionInfoMessage;
			}
			else
			{
				_oleDbConnection.InfoMessage -= OleDbConnectionInfoMessage;
			}
		}
		#endregion
	}
}
