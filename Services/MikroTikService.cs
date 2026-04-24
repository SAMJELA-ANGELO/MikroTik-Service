using tik4net;
using tik4net.Api;
using tik4net.Objects;
using tik4net.Objects.Ip.Hotspot;
using tik4net.Objects.System;
using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace MikrotikService.Services
{
    public class MikrotikService
    {
        private readonly string user = "admin";
        private readonly string pass = "To2day";
        
        // Multi-router support (for failover)
        private readonly string[] routerIPs = new[] { "10.0.0.3", "10.0.0.2" }; // School: 10.0.0.3, Home: 10.0.0.2
        private readonly string[] routerNames = new[] { "School", "Home" }; // Router names for logging

        // Connection pool - keeps connections alive and reuses them
        private static readonly Dictionary<string, ITikConnection> ConnectionPool = new();
        private static readonly ReaderWriterLockSlim ConnectionPoolLock = new();
        
        private static string? _activeRouterIp = null;
        private static DateTime _lastDiscoveryTime = DateTime.MinValue;
        private static readonly TimeSpan CacheDuration = TimeSpan.FromHours(1);
        private static readonly TimeSpan DefaultConnectionTimeout = TimeSpan.FromSeconds(3);
        private static readonly SemaphoreSlim _routerDiscoveryLock = new SemaphoreSlim(1, 1);
        private static readonly object _cacheLock = new object();

        private readonly ILogger<MikrotikService> _logger;

        public MikrotikService(ILogger<MikrotikService> logger)
        {
            _logger = logger;
            _logger.LogInformation("🔧 MikrotikService initialized");
        }

        /// <summary>
        /// Get or create a cached connection for the given router IP
        /// </summary>
        private ITikConnection GetOrCreateConnection(string ipAddress)
        {
            ConnectionPoolLock.EnterReadLock();
            try
            {
                if (ConnectionPool.TryGetValue(ipAddress, out var cachedConnection))
                {
                    try
                    {
                        // Test if connection is still alive by checking if it's connected
                        if (cachedConnection != null && IsConnectionAlive(cachedConnection))
                        {
                            _logger.LogInformation("♻️ Reusing cached connection to {ip}", ipAddress);
                            return cachedConnection;
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "⚠️ Cached connection to {ip} is dead, will recreate", ipAddress);
                    }
                }
            }
            finally
            {
                ConnectionPoolLock.ExitReadLock();
            }

            // Connection not found or dead, create new one
            ConnectionPoolLock.EnterWriteLock();
            try
            {
                // Double-check pattern - another thread might have created it
                if (ConnectionPool.TryGetValue(ipAddress, out var cachedConnection) && 
                    IsConnectionAlive(cachedConnection))
                {
                    return cachedConnection;
                }

                _logger.LogInformation("🆕 Creating new connection to {ip}", ipAddress);
                var newConnection = Connect(ipAddress);
                ConnectionPool[ipAddress] = newConnection;
                return newConnection;
            }
            finally
            {
                ConnectionPoolLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Check if a connection is still alive
        /// </summary>
        private bool IsConnectionAlive(ITikConnection connection)
        {
            try
            {
                // Try to execute a simple command to check if connection is alive
                var testCmd = connection.CreateCommand("/system/identity/print");
                testCmd.ExecuteList().FirstOrDefault();
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Remove a connection from the pool (when it fails)
        /// </summary>
        private void RemoveConnectionFromPool(string ipAddress)
        {
            ConnectionPoolLock.EnterWriteLock();
            try
            {
                if (ConnectionPool.TryGetValue(ipAddress, out var conn))
                {
                    try
                    {
                        conn?.Dispose();
                    }
                    catch { }
                    ConnectionPool.Remove(ipAddress);
                    _logger.LogInformation("🗑️ Removed dead connection from pool: {ip}", ipAddress);
                }
            }
            finally
            {
                ConnectionPoolLock.ExitWriteLock();
            }
        }

        private ITikConnection Connect(string ipAddress)
        {
            _logger.LogInformation("📡 Attempting to connect to MikroTik: {ip} with enforced timeout {timeoutSeconds}s", ipAddress, DefaultConnectionTimeout.TotalSeconds);
            var connection = ConnectionFactory.CreateConnection(TikConnectionType.Api);
            TrySetConnectionTimeout(connection, DefaultConnectionTimeout);

            var connectTask = Task.Run(() => connection.Open(ipAddress, user, pass));
            if (!connectTask.Wait(DefaultConnectionTimeout))
            {
                try
                {
                    connection.Dispose();
                }
                catch
                {
                }

                var timeoutMessage = $"Connection to {ipAddress} timed out after {DefaultConnectionTimeout.TotalSeconds} seconds.";
                _logger.LogError(timeoutMessage);
                throw new TimeoutException(timeoutMessage);
            }

            if (connectTask.IsFaulted)
            {
                var ex = connectTask.Exception?.GetBaseException() ?? new Exception("Connection failed during attempt.");
                _logger.LogError(ex, "❌ Failed to connect to MikroTik {ip}: {message}", ipAddress, ex.Message);
                throw ex;
            }

            _logger.LogInformation("✅ Connected to MikroTik {ip} successfully", ipAddress);
            return connection;
        }

        private void TrySetConnectionTimeout(ITikConnection connection, TimeSpan timeout)
        {
            if (connection == null)
                return;

            try
            {
                var connectionType = connection.GetType();
                var timeoutMs = (int)timeout.TotalMilliseconds;
                var timeoutProperty = connectionType.GetProperty("Timeout")
                                      ?? connectionType.GetProperty("ReadTimeout")
                                      ?? connectionType.GetProperty("WriteTimeout");

                if (timeoutProperty != null && timeoutProperty.CanWrite)
                {
                    if (timeoutProperty.PropertyType == typeof(int))
                    {
                        timeoutProperty.SetValue(connection, timeoutMs);
                        _logger.LogInformation("⏱️ Set tik4net timeout to {timeoutMs}ms", timeoutMs);
                    }
                    else if (timeoutProperty.PropertyType == typeof(TimeSpan))
                    {
                        timeoutProperty.SetValue(connection, timeout);
                        _logger.LogInformation("⏱️ Set tik4net timeout to {timeout}", timeout);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ Unable to set connection timeout on tik4net connection");
            }
        }

        private void OpenConnectionWithTimeout(ITikConnection connection, string host, string username, string password, TimeSpan timeout)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            TrySetConnectionTimeout(connection, timeout);
            var connectionType = connection.GetType();
            var openMethods = connectionType.GetMethods(BindingFlags.Instance | BindingFlags.Public)
                .Where(m => m.Name == "Open" && m.GetParameters().Length == 4)
                .ToList();

            foreach (var method in openMethods)
            {
                var parameters = method.GetParameters();
                if (parameters[0].ParameterType != typeof(string) ||
                    parameters[1].ParameterType != typeof(string) ||
                    parameters[2].ParameterType != typeof(string))
                {
                    continue;
                }

                var timeoutParamType = parameters[3].ParameterType;
                object timeoutValue;
                if (timeoutParamType == typeof(TimeSpan))
                {
                    timeoutValue = timeout;
                }
                else if (timeoutParamType == typeof(int) || timeoutParamType == typeof(long) ||
                         timeoutParamType == typeof(uint) || timeoutParamType == typeof(ushort) ||
                         timeoutParamType == typeof(short))
                {
                    timeoutValue = (int)timeout.TotalMilliseconds;
                }
                else if (timeoutParamType == typeof(double) || timeoutParamType == typeof(float))
                {
                    timeoutValue = timeout.TotalSeconds;
                }
                else
                {
                    continue;
                }

                try
                {
                    method.Invoke(connection, new object[] { host, username, password, timeoutValue });
                    _logger.LogInformation("⏱️ Opened connection to {host} using explicit timeout overload", host);
                    return;
                }
                catch (TargetInvocationException tie)
                {
                    throw tie.InnerException ?? tie;
                }
            }

            OpenConnectionWithTaskTimeout(connection, host, username, password, timeout);
        }

        private void OpenConnectionWithTaskTimeout(ITikConnection connection, string host, string username, string password, TimeSpan timeout)
        {
            var connectTask = Task.Run(() =>
            {
                connection.Open(host, username, password);
                return connection;
            });

            if (!connectTask.Wait(timeout))
            {
                try
                {
                    connection.Dispose();
                }
                catch { }

                throw new TimeoutException($"Connection to {host} timed out after {timeout.TotalSeconds} seconds.");
            }

            if (connectTask.IsFaulted)
            {
                throw connectTask.Exception?.GetBaseException() ?? new Exception("Connection failed during attempt.");
            }
        }

        private IEnumerable<(string routerIP, string routerName)> GetRouterAttemptOrder()
        {
            var attemptedIps = new HashSet<string>();
            string? activeRouterIp;
            DateTime lastDiscoveryTime;

            lock (_cacheLock)
            {
                activeRouterIp = _activeRouterIp;
                lastDiscoveryTime = _lastDiscoveryTime;
            }

            if (!string.IsNullOrWhiteSpace(activeRouterIp) && (DateTime.UtcNow - lastDiscoveryTime) < CacheDuration)
            {
                attemptedIps.Add(activeRouterIp);
                yield return (activeRouterIp, GetRouterName(activeRouterIp));
            }

            for (int i = 0; i < routerIPs.Length; i++)
            {
                if (attemptedIps.Contains(routerIPs[i]))
                    continue;

                yield return (routerIPs[i], routerNames.Length > i ? routerNames[i] : $"Router-{i}");
            }
        }

        private string GetRouterName(string ipAddress)
        {
            for (int i = 0; i < routerIPs.Length; i++)
            {
                if (routerIPs[i] == ipAddress)
                    return routerNames.Length > i ? routerNames[i] : $"Router-{i}";
            }
            return ipAddress;
        }

        private bool TryGetCachedRouter(out string? routerIP)
        {
            lock (_cacheLock)
            {
                if (!string.IsNullOrWhiteSpace(_activeRouterIp) && (DateTime.UtcNow - _lastDiscoveryTime) < CacheDuration)
                {
                    routerIP = _activeRouterIp;
                    return true;
                }

                routerIP = null;
                return false;
            }
        }

        private void SetActiveRouter(string routerIP)
        {
            lock (_cacheLock)
            {
                _activeRouterIp = routerIP;
                _lastDiscoveryTime = DateTime.UtcNow;
            }
        }

        private void ClearActiveRouterCache()
        {
            lock (_cacheLock)
            {
                _activeRouterIp = null;
                _lastDiscoveryTime = DateTime.MinValue;
            }
        }

        private bool IsEmptyResponseException(Exception? ex)
        {
            while (ex != null)
            {
                var msg = ex.Message ?? string.Empty;
                if (msg.Contains("!empty"))
                    return true;
                ex = ex.InnerException;
            }
            return false;
        }

        /// <summary>
        /// Safe wrapper for ExecuteList that treats !empty response as empty results instead of throwing
        /// </summary>
        private IEnumerable<ITikReSentence> SafeExecuteList(ITikCommand cmd)
        {
            try
            {
                return cmd.ExecuteList();
            }
            catch (Exception ex) when (IsEmptyResponseException(ex))
            {
                // MikroTik returned !empty - no results found, which is fine
                // Treat this as an empty list instead of crashing
                return Enumerable.Empty<ITikReSentence>();
            }
        }

        public async Task<string> ActivateUser(string username, int durationHours)
        {
            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException("Username is required");

            if (TryGetCachedRouter(out var cachedRouterIp))
            {
                try
                {
                    _logger.LogInformation("🔄 [Sticky] Trying cached router {routerIP} first for user {username}", cachedRouterIp, username);
                    _logger.LogInformation("✅ [Sticky] Fast path hit: using cached router {routerIP} for user {username}", cachedRouterIp, username);
                    return await ActivateUserOnRouter(cachedRouterIp!, username, durationHours).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "⚠️ Cached router {routerIP} failed, clearing sticky cache and falling back", cachedRouterIp);
                    ClearActiveRouterCache();
                }
            }

            await _routerDiscoveryLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (TryGetCachedRouter(out cachedRouterIp))
                {
                    try
                    {
                        _logger.LogInformation("🔄 [Sticky] Re-checking cached router {routerIP} before failover for user {username}", cachedRouterIp, username);
                        return await ActivateUserOnRouter(cachedRouterIp!, username, durationHours).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "⚠️ Cached router {routerIP} failed on re-check, clearing sticky cache and continuing failover", cachedRouterIp);
                        ClearActiveRouterCache();
                    }
                }

                for (int i = 0; i < routerIPs.Length; i++)
                {
                    string routerIP = routerIPs[i];
                    string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                    try
                    {
                        _logger.LogInformation("🔄 [{routerName}] Attempting to activate user {username}...", routerName, username);
                        return await ActivateUserOnRouter(routerIP, username, durationHours).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "❌ [{routerName}] Activation failed: {message}", routerName, ex.Message);
                        // Continue to next router
                    }
                }

                throw new Exception($"Failed to activate user '{username}' on any available router.");
            }
            finally
            {
                _routerDiscoveryLock.Release();
            }
        }

        private async Task<string> ActivateUserOnRouter(string routerIP, string username, int durationHours)
        {
            string routerName = GetRouterName(routerIP);
            _logger.LogInformation("🔄 [{routerName}] Activating user {username} on router {routerIP}...", routerName, username, routerIP);

            ITikConnection connection = GetOrCreateConnection(routerIP);
            try
            {
                List<HotspotUser> users;
                try
                {
                    users = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).ToList();
                }
                catch (Exception ex)
                {
                    if (IsEmptyResponseException(ex))
                    {
                        users = new List<HotspotUser>();
                    }
                    else
                        throw;
                }

                var profile = $"profile-{durationHours}h";

                // Ensure the requested profile exists on the device before assigning it.
                await EnsureProfileExistsOnConnectionAsync(connection, profile).ConfigureAwait(false);

                if (!HotspotProfileExists(connection, profile))
                {
                    _logger.LogError("❌ [{routerName}] Profile {profile} was not found after creation", routerName, profile);
                    throw new InvalidOperationException($"Hotspot profile '{profile}' could not be verified on router {routerIP}.");
                }

                if (users.Count == 0)
                {
                    _logger.LogWarning("⚠️ [{routerName}] User {username} not found", routerName, username);
                    throw new InvalidOperationException($"User '{username}' not found on router {routerIP}.");
                }

                var user = users.First();
                _logger.LogInformation("🔍 [{routerName}] Found user {username} with ID: '{userId}'", routerName, username, user.Id ?? "null");
                if (string.IsNullOrWhiteSpace(user.Id))
                {
                    _logger.LogError("❌ [{routerName}] Cannot activate user {username}: missing MikroTik internal .id", routerName, username);
                    throw new InvalidOperationException($"Hotspot user '{username}' has no internal .id on router {routerName}.");
                }

                var setCommand = connection.CreateCommand("/ip/hotspot/user/set");
                setCommand.AddParameter(".id", user.Id); // MUST be first for /set operations
                _logger.LogInformation("   🔧 Executing user set with .id={userId}", user.Id);
                setCommand.AddParameter("profile", profile);
                setCommand.AddParameter("limit-uptime", $"{durationHours}h");
                setCommand.AddParameter("disabled", "no");

                try
                {
                    setCommand.ExecuteList();
                    _logger.LogInformation("✅ [{routerName}] User {username} activated with {hours}h access", routerName, username, durationHours);
                }
                catch (Exception ex)
                {
                    if (!IsEmptyResponseException(ex))
                        throw;
                    _logger.LogInformation("ℹ️ [{routerName}] SET command returned empty response for {username}", routerName, username);
                }

                // Verification: ensure the user now exists on the device
                try
                {
                    var verified = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).FirstOrDefault();
                    if (verified == null)
                    {
                        _logger.LogError("❌ [{routerName}] Activation verification failed: user '{username}' not found after operation", routerName, username);
                        throw new InvalidOperationException($"Failed to verify activation for user '{username}' on router {routerIP}.");
                    }

                    SetActiveRouter(routerIP);
                    _logger.LogInformation("✅ [{routerName}] Activation verified for {username}", routerName, username);
                    return routerName;
                }
                catch (Exception ex)
                {
                    if (IsEmptyResponseException(ex))
                        throw new InvalidOperationException("Activation inconclusive: device returned '!empty' during verification.");
                    throw;
                }
            }
            catch (Exception ex)
            {
                // If connection fails, remove it from pool so a fresh one is created next time
                _logger.LogWarning(ex, "❌ Operation failed on router {routerIP}, removing from connection pool", routerIP);
                RemoveConnectionFromPool(routerIP);
                throw;
            }
        }


        private bool IsHotspotHostEntryPresent(ITikConnection connection, string macAddress, string ip)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            if (string.IsNullOrWhiteSpace(macAddress) || string.IsNullOrWhiteSpace(ip))
                return false;

            try
            {
                // Use dynamic since HotspotHost type may not be available in tik4net
                var hosts = connection.LoadList<dynamic>(
                    connection.CreateParameter("mac-address", macAddress),
                    connection.CreateParameter("address", ip)
                ).ToList();

                var found = hosts.Any();
                Console.WriteLine($"🔍 Hotspot host lookup: mac={macAddress}, ip={ip}, found={found}");
                return found;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️ Hotspot host lookup failed: {ex.Message}");
                return false;
            }
        }

        private void MoveHostToActive(ITikConnection connection, string username, string password, string macAddress, string ip)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            if (string.IsNullOrWhiteSpace(username) ||
                string.IsNullOrWhiteSpace(password) ||
                string.IsNullOrWhiteSpace(macAddress) ||
                string.IsNullOrWhiteSpace(ip))
            {
                throw new ArgumentException("Username, password, MAC address, and IP are required for forced login.");
            }

            // Verify that the host entry exists in the hotspot host list (by MAC and IP)
            if (!IsHotspotHostEntryPresent(connection, macAddress, ip))
            {
                throw new InvalidOperationException($"Host entry not found for MAC {macAddress} and IP {ip}. Ensure device appears in /ip/hotspot/host first.");
            }

            var loginCmd = connection.CreateCommand("/ip/hotspot/active/login");
            loginCmd.AddParameter("user", username);
            loginCmd.AddParameter("password", password);
            loginCmd.AddParameter("mac-address", macAddress);
            loginCmd.AddParameter("ip", ip);

            Console.WriteLine($"📡 Force login (Active) command: user={username}, mac={macAddress}, ip={ip}");

            try
            {
                SafeExecuteList(loginCmd);
                Console.WriteLine("✅ User is now Active (forced login)");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Forced login failed: {ex.Message}");
                throw;
            }
        }

        public void MoveHostToActive(string username, string password, string macAddress, string ip)
        {
            if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password) ||
                string.IsNullOrWhiteSpace(macAddress) || string.IsNullOrWhiteSpace(ip))
            {
                throw new ArgumentException("Username, password, MAC address, and IP are required");
            }

            // Try each router until one works
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                try
                {
                    _logger.LogInformation("🔄 [{routerName}] Attempting to move host to active for {username}...", routerName, username);
                    var connection = GetOrCreateConnection(routerIP);
                    MoveHostToActive(connection, username, password, macAddress, ip);
                    _logger.LogInformation("✅ [{routerName}] Host moved to active for {username}", routerName, username);
                    return; // Success - exit the method
                }
                catch (Exception ex)
                {
                    RemoveConnectionFromPool(routerIP);
                    _logger.LogError(ex, "❌ [{routerName}] Failed to move host to active for {username}: {message}", routerName, username, ex.Message);
                    // Continue to next router
                }
            }

            throw new Exception($"Failed to move host to active for {username} on any available router.");
        }

        public void DeactivateUser(string username)
        {
            if (string.IsNullOrWhiteSpace(username))
            {
                throw new ArgumentException("Username is required");
            }

            // Try each router until one works
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                try
                {
                    _logger.LogInformation("🔄 [{routerName}] Attempting to deactivate user {username}...", routerName, username);
                    var connection = GetOrCreateConnection(routerIP);

                    var users = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).ToList();

                    if (users.Count > 0)
                    {
                        var user = users.First();
                        var removeCommand = connection.CreateCommand("/ip/hotspot/user/remove");
                        removeCommand.AddParameter(".id", user.Id);

                        try
                        {
                            removeCommand.ExecuteNonQuery();
                            _logger.LogInformation("✅ [{routerName}] User {username} deactivated successfully", routerName, username);
                            return; // Success - exit the method
                        }
                        catch (Exception ex)
                        {
                            if (IsEmptyResponseException(ex))
                            {
                                _logger.LogInformation("✅ [{routerName}] User {username} deactivation verified (empty response)", routerName, username);
                                return; // Success - exit the method
                            }
                            throw;
                        }
                    }
                    else
                    {
                        _logger.LogWarning("⚠️ [{routerName}] User {username} not found, trying next router...", routerName, username);
                        // Continue to next router
                    }
                }
                catch (Exception ex)
                {
                    RemoveConnectionFromPool(routerIP);
                    _logger.LogError(ex, "❌ [{routerName}] Failed to deactivate user {username}: {message}", routerName, username, ex.Message);
                    // Continue to next router
                }
            }

            throw new Exception($"Failed to deactivate user {username} on any available router.");
        }

        public async Task CreateUser(string username, string password)
        {
            if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
            {
                throw new ArgumentException("Username and password are required");
            }

            // Try each router until one works
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                try
                {
                    _logger.LogInformation("🔄 [{routerName}] Attempting to create user {username}...", routerName, username);
                    var connection = GetOrCreateConnection(routerIP);

                    // Ensure the blocked profile exists
                    await EnsureProfileExistsOnConnectionAsync(connection, "profile-blocked").ConfigureAwait(false);

                    var addCommand = connection.CreateCommand("/ip/hotspot/user/add");
                    addCommand.AddParameter("name", username);
                    addCommand.AddParameter("password", password);
                    addCommand.AddParameter("profile", "profile-blocked");
                    addCommand.AddParameter("disabled", "yes");

                    // Try to add the user; if tik4net throws the known '!empty' response error,
                    // continue to verification step instead of failing immediately.
                    SafeExecuteList(addCommand);
                    _logger.LogInformation("✅ [{routerName}] User {username} created with blocked access and disabled until activation", routerName, username);

                    // Verify the user exists. If verification fails with '!empty', consider it success.
                    try
                    {
                        var created = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).FirstOrDefault();
                        if (created == null)
                        {
                            throw new InvalidOperationException($"Failed to verify creation of user '{username}'");
                        }
                        _logger.LogInformation("✅ [{routerName}] User {username} creation verified", routerName, username);
                        return; // Success - exit the method
                    }
                    catch (Exception ex)
                    {
                        if (IsEmptyResponseException(ex))
                        {
                            _logger.LogInformation("✅ [{routerName}] User {username} creation verified (empty response)", routerName, username);
                            return; // assume success when device responds with '!empty'
                        }
                        throw;
                    }
                }
                catch (Exception ex)
                {
                    RemoveConnectionFromPool(routerIP);
                    _logger.LogError(ex, "❌ [{routerName}] Failed to create user {username}: {message}", routerName, username, ex.Message);
                    // Continue to next router
                }
            }

            throw new Exception($"Failed to create user {username} on any available router.");
        }

        public void DeleteUser(string username)
        {
            if (string.IsNullOrWhiteSpace(username))
            {
                throw new ArgumentException("Username is required");
            }

            // Try each router until one works
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                try
                {
                    _logger.LogInformation("🔄 [{routerName}] Attempting to delete user {username}...", routerName, username);
                    var connection = GetOrCreateConnection(routerIP);

                    var user = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).FirstOrDefault();
                    if (user == null)
                    {
                        _logger.LogWarning("⚠️ [{routerName}] User {username} not found, trying next router...", routerName, username);
                        continue; // Try next router
                    }

                    var removeCommand = connection.CreateCommand("/ip/hotspot/user/remove");
                    removeCommand.AddParameter(".id", user.Id);
                    SafeExecuteList(removeCommand);

                    _logger.LogInformation("✅ [{routerName}] User {username} deleted successfully", routerName, username);
                    return; // Success - exit the method
                }
                catch (Exception ex)
                {
                    RemoveConnectionFromPool(routerIP);
                    if (IsEmptyResponseException(ex))
                    {
                        _logger.LogInformation("✅ [{routerName}] User {username} deletion verified (empty response)", routerName, username);
                        return; // Success - exit the method
                    }
                    _logger.LogError(ex, "❌ [{routerName}] Failed to delete user {username}: {message}", routerName, username, ex.Message);
                    // Continue to next router
                }
            }

            throw new Exception($"Failed to delete user {username} on any available router.");
        }

        public Task UpdateUserPassword(string username, string newPassword)
        {
            if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(newPassword))
            {
                throw new ArgumentException("Username and new password are required");
            }

            // Try each router until one works
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                try
                {
                    _logger.LogInformation("🔄 [{routerName}] Attempting to update password for user {username}...", routerName, username);
                    var connection = GetOrCreateConnection(routerIP);

                    var user = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).FirstOrDefault();
                    if (user == null)
                    {
                        _logger.LogWarning("⚠️ [{routerName}] User {username} not found, trying next router...", routerName, username);
                        continue; // Try next router
                    }

                    var setCommand = connection.CreateCommand("/ip/hotspot/user/set");
                    setCommand.AddParameter(".id", user.Id);
                    setCommand.AddParameter("password", newPassword);
                    SafeExecuteList(setCommand);

                    _logger.LogInformation("✅ [{routerName}] Password updated for user {username}", routerName, username);
                    return Task.CompletedTask; // Success - exit the method
                }
                catch (Exception ex)
                {
                    if (IsEmptyResponseException(ex))
                    {
                        _logger.LogInformation("✅ [{routerName}] Password update verified for user {username} (empty response)", routerName, username);
                        return Task.CompletedTask; // Success - exit the method
                    }
                    _logger.LogError(ex, "❌ [{routerName}] Failed to update password for user {username}: {message}", routerName, username, ex.Message);
                    // Continue to next router
                }
            }

            throw new Exception($"Failed to update password for user {username} on any available router.");
        }

        public void DisableUser(string username)
        {
            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException("Username is required");

            // Try each router until one works
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                try
                {
                    _logger.LogInformation("🔄 [{routerName}] Attempting to disable user {username}...", routerName, username);
                    var connection = GetOrCreateConnection(routerIP);

                    var users = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).ToList();

                    if (users.Count == 0)
                    {
                        _logger.LogWarning("⚠️ [{routerName}] User {username} not found, trying next router...", routerName, username);
                        continue; // Try next router
                    }

                    var user = users.First();
                    _logger.LogInformation("🔍 [{routerName}] Found user {username} with ID: '{userId}'", routerName, username, user.Id ?? "null");
                    if (string.IsNullOrWhiteSpace(user.Id))
                    {
                        _logger.LogError("❌ [{routerName}] Cannot disable user {username}: missing MikroTik internal .id", routerName, username);
                        throw new InvalidOperationException($"Hotspot user '{username}' has no internal .id on router {routerName}.");
                    }

                    var setCommand = connection.CreateCommand("/ip/hotspot/user/set");
                    setCommand.AddParameter(".id", user.Id); // MUST be first for /set operations
                    _logger.LogInformation("   🔧 Executing disable set with .id={userId}", user.Id);
                    setCommand.AddParameter("disabled", "yes");

                    try
                    {
                        setCommand.ExecuteList();
                        _logger.LogInformation("✅ [{routerName}] User {username} disabled successfully", routerName, username);
                    }
                    catch (Exception ex)
                    {
                        if (!IsEmptyResponseException(ex))
                            throw;
                        // else continue
                    }

                    // Verify the user is now disabled
                    var verified = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).FirstOrDefault();
                    if (verified != null)
                    {
                        _logger.LogInformation("✅ [{routerName}] User {username} disable verified", routerName, username);
                        return; // Success - exit the method
                    }
                }
                catch (Exception ex)
                {
                    RemoveConnectionFromPool(routerIP);
                    if (IsEmptyResponseException(ex))
                    {
                        _logger.LogInformation("✅ [{routerName}] User {username} disable verified (empty response)", routerName, username);
                        return; // Success - exit the method
                    }
                    _logger.LogError(ex, "❌ [{routerName}] Failed to disable user {username}: {message}", routerName, username, ex.Message);
                    // Continue to next router
                }
            }

            throw new Exception($"Failed to disable user {username} on any available router.");
        }

        public object TestConnection()
        {
            // Try each router until one works
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                try
                {
                    _logger.LogInformation("🔄 [{routerName}] Testing connection...", routerName);
                    var connection = GetOrCreateConnection(routerIP);
                    var identity = connection.LoadAll<SystemIdentity>().First();
                    _logger.LogInformation("✅ [{routerName}] Connection test successful: {identity}", routerName, identity.Name);
                    return new { connected = true, identity = identity.Name, router = routerName };
                }
                catch (Exception ex)
                {
                    RemoveConnectionFromPool(routerIP);
                    _logger.LogError(ex, "❌ [{routerName}] Connection test failed: {message}", routerName, ex.Message);
                    // Continue to next router
                }
            }

            return new { connected = false, error = "No routers available" };
        }

        public List<HotspotUser> ListHotspotUsers()
        {
            // Try each router until one works
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                try
                {
                    _logger.LogInformation("🔄 [{routerName}] Attempting to list hotspot users...", routerName);
                    var connection = GetOrCreateConnection(routerIP);
                    var users = connection.LoadAll<HotspotUser>().ToList();
                    _logger.LogInformation("✅ [{routerName}] Retrieved {count} hotspot users", routerName, users.Count);
                    return users;
                }
                catch (Exception ex)
                {
                    RemoveConnectionFromPool(routerIP);
                    _logger.LogError(ex, "❌ [{routerName}] Failed to list hotspot users: {message}", routerName, ex.Message);
                    // Continue to next router
                }
            }

            throw new Exception("Failed to list hotspot users from any available router.");
        }

        public HotspotUser? GetUserDetails(string username)
        {
            if (string.IsNullOrWhiteSpace(username))
            {
                throw new ArgumentException("Username is required");
            }

            // Try each router until one works
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                try
                {
                    _logger.LogInformation("🔄 [{routerName}] Attempting to get user details for {username}...", routerName, username);
                    var connection = GetOrCreateConnection(routerIP);
#pragma warning disable CS8603
                    var user = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).FirstOrDefault();
#pragma warning restore CS8603
                    if (user != null)
                    {
                        _logger.LogInformation("✅ [{routerName}] Retrieved user details for {username}", routerName, username);
                        return user;
                    }
                    else
                    {
                        _logger.LogWarning("⚠️ [{routerName}] User {username} not found, trying next router...", routerName, username);
                        // Continue to next router
                    }
                }
                catch (Exception ex)
                {
                    RemoveConnectionFromPool(routerIP);
                    _logger.LogError(ex, "❌ [{routerName}] Failed to get user details for {username}: {message}", routerName, username, ex.Message);
                    // Continue to next router
                }
            }

            return null; // User not found on any router
        }

        public List<HotspotActive> GetActiveUsers()
        {
            // Try each router until one works
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                try
                {
                    _logger.LogInformation("🔄 [{routerName}] Attempting to get active users...", routerName);
                    var connection = GetOrCreateConnection(routerIP);
                    var activeUsers = connection.LoadAll<HotspotActive>().ToList();
                    _logger.LogInformation("✅ [{routerName}] Retrieved {count} active users", routerName, activeUsers.Count);
                    return activeUsers;
                }
                catch (Exception ex)
                {
                    RemoveConnectionFromPool(routerIP);
                    _logger.LogError(ex, "❌ [{routerName}] Failed to get active users: {message}", routerName, ex.Message);
                    // Continue to next router
                }
            }

            throw new Exception("Failed to get active users from any available router.");
        }

        public void BindMacToBypass(string macAddress, int durationHours)
        {
            if (string.IsNullOrWhiteSpace(macAddress))
                throw new ArgumentException("MAC address is required");

            // Try each router until one works
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                try
                {
                    _logger.LogInformation("🔄 [{routerName}] Attempting to bind MAC {macAddress} to bypass...", routerName, macAddress);
                    var connection = GetOrCreateConnection(routerIP);

                    // Check if binding already exists
                    var existingBindings = connection.LoadList<dynamic>(
                        connection.CreateParameter("mac-address", macAddress)
                    ).ToList();

                    // Remove old binding if exists
                    if (existingBindings != null && existingBindings.Count > 0)
                    {
                        var bindingCommand = connection.CreateCommand("/ip/hotspot/ip-binding/remove");
                        bindingCommand.AddParameter(".id", existingBindings[0].Id);
                        try
                        {
                            bindingCommand.ExecuteNonQuery();
                            _logger.LogInformation("✅ [{routerName}] Removed existing binding for MAC {macAddress}", routerName, macAddress);
                        }
                        catch (Exception ex)
                        {
                            if (!IsEmptyResponseException(ex))
                                throw;
                        }
                    }

                    // Add new binding with bypass type
                    var addCommand = connection.CreateCommand("/ip/hotspot/ip-binding/add");
                    addCommand.AddParameter("mac-address", macAddress);
                    addCommand.AddParameter("type", "bypassed");
                    if (durationHours > 0)
                    {
                        addCommand.AddParameter("timeout", $"{durationHours}h");
                    }

                    try
                    {
                        addCommand.ExecuteNonQuery();
                        _logger.LogInformation("✅ [{routerName}] MAC {macAddress} bound to bypass successfully", routerName, macAddress);
                        return; // Success - exit the method
                    }
                    catch (Exception ex)
                    {
                        if (!IsEmptyResponseException(ex))
                            throw;
                        // else continue
                    }
                }
                catch (Exception ex)
                {
                    RemoveConnectionFromPool(routerIP);
                    _logger.LogError(ex, "❌ [{routerName}] Failed to bind MAC {macAddress} to bypass: {message}", routerName, macAddress, ex.Message);
                    // Continue to next router
                }
            }

            throw new Exception($"Failed to bind MAC address {macAddress} to bypass on any available router.");
        }

        public void UnbindMac(string macAddress)
        {
            if (string.IsNullOrWhiteSpace(macAddress))
                throw new ArgumentException("MAC address is required");

            // Try each router until one works
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                try
                {
                    _logger.LogInformation("🔄 [{routerName}] Attempting to unbind MAC {macAddress}...", routerName, macAddress);
                    var connection = GetOrCreateConnection(routerIP);

                    // Find and remove the binding
                    var bindings = connection.LoadList<dynamic>(
                        connection.CreateParameter("mac-address", macAddress)
                    ).ToList();

                    if (bindings != null && bindings.Count > 0)
                    {
                        var removeCommand = connection.CreateCommand("/ip/hotspot/ip-binding/remove");
                        removeCommand.AddParameter(".id", bindings[0].Id);

                        try
                        {
                            removeCommand.ExecuteNonQuery();
                            _logger.LogInformation("✅ [{routerName}] MAC {macAddress} unbound successfully", routerName, macAddress);
                            return; // Success - exit the method
                        }
                        catch (Exception ex)
                        {
                            if (!IsEmptyResponseException(ex))
                                throw;
                            // else continue
                        }
                    }
                    else
                    {
                        _logger.LogWarning("⚠️ [{routerName}] MAC {macAddress} binding not found, trying next router...", routerName, macAddress);
                        // Continue to next router
                    }
                }
                catch (Exception ex)
                {
                    RemoveConnectionFromPool(routerIP);
                    _logger.LogError(ex, "❌ [{routerName}] Failed to unbind MAC {macAddress}: {message}", routerName, macAddress, ex.Message);
                    // Continue to next router
                }
            }

            throw new Exception($"Failed to unbind MAC address {macAddress} on any available router.");
        }

        /// <summary>
        /// FAILOVER METHOD: Tries to activate a user on the first available router (Home then School)
        /// Used for the "mobile Starlink" scenario where only one router is online at a time
        /// </summary>
        public async Task<string> ActivateOnAvailableRouter(string username, int durationHours, string? macAddress = null)
        {
            _logger.LogInformation("🚀 ActivateOnAvailableRouter START: username={username}, duration={durationHours}h, mac={macAddress}", username, durationHours, macAddress ?? "null");
            
            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException("Username is required");

            List<string> errors = new List<string>();
            string? successfulRouter = null;

            foreach (var candidate in GetRouterAttemptOrder())
            {
                bool isStickyCandidate = candidate.routerIP == _activeRouterIp;
                _logger.LogInformation("🔄 [{routerName}] Attempting to activate {username}...", candidate.routerName, username);

                try
                {
                    var connection = GetOrCreateConnection(candidate.routerIP);
                    SetActiveRouter(candidate.routerIP);
                    _logger.LogInformation("✅ [{routerName}] Connected successfully", candidate.routerName);

                    _logger.LogInformation("   Step 1/2: Creating/updating hotspot user...");
                    await ActivateUserOnConnection(connection, username, durationHours).ConfigureAwait(false);
                    _logger.LogInformation("   ✅ Hotspot user ready");

                    successfulRouter = candidate.routerName;
                    _logger.LogInformation("✅ [{routerName}] Successfully activated {username}", candidate.routerName, username);

                    if (!string.IsNullOrWhiteSpace(macAddress))
                    {
                        try
                        {
                            _logger.LogInformation("   Step 2/2: Binding MAC address {macAddress}...", macAddress);
                            BindMacOnConnection(connection, macAddress, durationHours);
                            _logger.LogInformation("   ✅ MAC address bound successfully");
                        }
                        catch (Exception macError)
                        {
                            _logger.LogWarning("⚠️ MAC binding warning (non-critical): {message}", macError.Message);
                        }
                    }

                    _logger.LogInformation("🚀 ActivateOnAvailableRouter SUCCESS on {routerName}", candidate.routerName);
                    return successfulRouter!;
                }
                catch (Exception ex)
                {
                    if (isStickyCandidate)
                    {
                        _logger.LogWarning(ex, "⚠️ Sticky router {routerIP} failed, clearing cache and trying next router...", candidate.routerIP);
                        ClearActiveRouterCache();
                    }
                    else
                    {
                        _logger.LogError(ex, "❌ [{routerName}] Activation failed: {message}", candidate.routerName, ex.Message);
                    }

                    errors.Add($"{candidate.routerName} ({candidate.routerIP}): {ex.Message}");
                    continue;
                }
            }

            string allErrors = string.Join("; ", errors);
            _logger.LogError("❌ Failed to activate {username} on any available router. Errors: {errors}", username, allErrors);
            throw new Exception(
                $"Failed to activate {username} on any available router. Errors:\n" +
                string.Join("\n", errors)
            );
        }

        /// <summary>
        /// GIFT FLOW METHOD: Creates hotspot user WITHOUT instant MAC binding
        /// Recipient will log in manually; MAC will be captured on first login
        /// Used when one user buys a bundle for another user
        /// </summary>
        public async Task<string> CreateHotspotUserOnly(string username, int durationHours)
        {
            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException("Username is required");

            List<string> errors = new List<string>();
            string? successfulRouter = null;

            // Try each router in order
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                try
                {
                    Console.WriteLine($"🎁 Attempting to create hotspot user {username} on {routerName} ({routerIP})...");
                    
                    var connection = GetOrCreateConnection(routerIP);
                    
                    // Try to activate the user (creates hotspot account with limit-uptime)
                    await ActivateUserOnConnection(connection, username, durationHours).ConfigureAwait(false);
                    
                    // If we get here, connection was successful
                    successfulRouter = routerName;
                    Console.WriteLine($"✅ Successfully created hotspot user {username} on {routerName}");
                    Console.WriteLine($"   Recipient will log in manually; MAC binding will happen on first login");

                    return successfulRouter;
                }
                catch (Exception ex)
                {
                    string errorMsg = $"{routerName} ({routerIP}): {ex.Message}";
                    errors.Add(errorMsg);
                    Console.WriteLine($"❌ Failed on {routerName}: {ex.Message}");
                    // Continue to next router
                }
            }

            // If we get here, all routers failed
            throw new Exception(
                $"Failed to create hotspot user {username} on any available router. Errors:\n" +
                string.Join("\n", errors)
            );
        }

        /// <summary>
        /// SILENT LOGIN METHOD: Forces an active login session for a user
        /// This moves the user from 'Hosts' to 'Active' on the MikroTik hotspot
        /// Used for automatic authentication after payment or web login
        /// </summary>
        public async Task<string> SilentLogin(string username, string password, string mac, string ip, int durationHours)
        {
            Console.WriteLine($"🔐 ===== SILENT LOGIN START =====");
            Console.WriteLine($"🔐 Username: {username}");
            Console.WriteLine($"🔐 Password: [HIDDEN]");
            Console.WriteLine($"🔐 MAC: {mac}");
            Console.WriteLine($"🔐 IP: {ip}");
            Console.WriteLine($"🔐 Duration Hours: {durationHours}");

            if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password) ||
                string.IsNullOrWhiteSpace(mac) || string.IsNullOrWhiteSpace(ip))
                throw new ArgumentException("Username, password, MAC address, and IP are required");

            List<string> errors = new List<string>();
            string? successfulRouter = null;

            // Try each router in order
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                Console.WriteLine($"🔄 Attempting silent login on {routerName} ({routerIP})...");

                try
                {
                    var connection = GetOrCreateConnection(routerIP);
                    Console.WriteLine($"✅ Connected to {routerName}");

                    // 1. Create/Update the Hotspot User first (so they exist in the DB)
                    Console.WriteLine($"   1️⃣ Creating/updating hotspot user...");
                    await ActivateUserOnConnection(connection, username, durationHours).ConfigureAwait(false);
                    Console.WriteLine($"   ✅ Hotspot user ready");

                    // 2. FORCE the login session for this specific device
                    // This makes the MikroTik move the user from 'Hosts' to 'Active'
                    Console.WriteLine($"   2️⃣ Forcing login session (user: {username}, mac: {mac}, ip: {ip})...");
                    MoveHostToActive(connection, username, password, mac, ip);
                    Console.WriteLine($"   ✅ Login session forced successfully by MoveHostToActive");

                    // If we get here, login was successful
                    SetActiveRouter(routerIP);
                    successfulRouter = routerName;
                    Console.WriteLine($"✅ Silent login successful for {username} on {routerName}");
                    Console.WriteLine($"🔐 ===== SILENT LOGIN SUCCESS =====");
                    return successfulRouter;
                }
                catch (Exception ex)
                {
                    string errorMsg = $"{routerName} ({routerIP}): {ex.Message}";
                    errors.Add(errorMsg);
                    Console.WriteLine($"❌ Silent login failed on {routerName}: {ex.Message}");
                    Console.WriteLine($"❌ Error details: {ex.ToString()}");
                    // Continue to next router
                }
            }

            // If we get here, all routers failed
            Console.WriteLine($"❌ ===== SILENT LOGIN FAILED - ALL ROUTERS FAILED =====");
            Console.WriteLine($"❌ Errors: {string.Join("; ", errors)}");
            throw new Exception(
                $"Failed to perform silent login for {username} on any available router. Errors:\n" +
                string.Join("\n", errors)
            );
        }
        public string BindMacOnAvailableRouter(string macAddress, int durationHours = 0)
        {
            _logger.LogInformation("📌 BindMacOnAvailableRouter START: mac={macAddress}, duration={durationHours}h", macAddress, durationHours);
            
            if (string.IsNullOrWhiteSpace(macAddress))
                throw new ArgumentException("MAC address is required");

            List<string> errors = new List<string>();

            // Try each router in order
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                try
                {
                    _logger.LogInformation("🔄 [{routerName}] Attempting to bind MAC {macAddress}...", routerName, macAddress);
                    
                    var connection = GetOrCreateConnection(routerIP);
                    _logger.LogInformation("✅ [{routerName}] Connected successfully", routerName);
                    
                    // Try to bind the MAC
                    BindMacOnConnection(connection, macAddress, durationHours);
                    
                    _logger.LogInformation("✅ [{routerName}] Successfully bound MAC {macAddress}", routerName, macAddress);
                    _logger.LogInformation("📌 BindMacOnAvailableRouter SUCCESS on {routerName}", routerName);
                    return routerName;
                }
                catch (Exception ex)
                {
                    string errorMsg = $"{routerName} ({routerIP}): {ex.Message}";
                    errors.Add(errorMsg);
                    _logger.LogError(ex, "❌ [{routerName}] MAC binding failed: {message}", routerName, ex.Message);
                    // Continue to next router
                }
            }

            // If we get here, all routers failed
            string allErrors = string.Join("; ", errors);
            _logger.LogError("❌ Failed to bind MAC {macAddress} on any available router. Errors: {errors}", macAddress, allErrors);
            throw new Exception(
                $"Failed to bind MAC {macAddress} on any available router. Errors:\n" +
                string.Join("\n", errors)
            );
        }

        /// <summary>
        /// FAILOVER METHOD: Tries to unbind MAC on Available routers (tries both Home and School)
        /// Used when session expires - MAC might exist on either router
        /// </summary>
        public void UnbindMacOnAvailableRouters(string macAddress)
        {
            if (string.IsNullOrWhiteSpace(macAddress))
                throw new ArgumentException("MAC address is required");

            Console.WriteLine($"🔍 Searching for MAC {macAddress} on available routers...");
            bool foundOnAnyRouter = false;

            // Try each router - don't fail if one doesn't have it
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                try
                {
                    Console.WriteLine($"🔄 [{routerName}] Searching for MAC binding...");
                    var connection = GetOrCreateConnection(routerIP);
                    
                    // Try to find and remove the binding using raw command instead of LoadList
                    // (LoadList fails due to HotspotIpBinding not having TikEntityAttribute)
                    var printCmd = connection.CreateCommand("/ip/hotspot/ip-binding/print");
                    printCmd.AddParameter(".proplist", ".id");
                    printCmd.AddParameter("?mac-address", macAddress);
                    
                    var bindings = SafeExecuteList(printCmd).ToList();
                    Console.WriteLine($"   Found {bindings.Count} binding(s) for MAC {macAddress}");

                    if (bindings != null && bindings.Count > 0)
                    {
                        var bindingId = bindings[0].GetResponseField(".id");
                        var removeCommand = connection.CreateCommand("/ip/hotspot/ip-binding/remove");
                        removeCommand.AddParameter(".id", bindingId);

                        try
                        {
                            Console.WriteLine($"   Executing REMOVE command for binding (ID: {bindingId})");
                            removeCommand.ExecuteNonQuery();
                            Console.WriteLine($"   ✅ Removed MAC binding from {routerName}");
                            foundOnAnyRouter = true;
                        }
                        catch (Exception ex)
                        {
                            if (!IsEmptyResponseException(ex))
                                throw;
                            Console.WriteLine("   Binding removed with empty response");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"   ℹ️ MAC not found on {routerName}");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"⚠️ [{routerName}] Could not check: {ex.Message}");
                    // Continue checking other routers
                }
            }

            if (!foundOnAnyRouter)
            {
                Console.WriteLine($"⚠️ MAC {macAddress} not found on any router (may already be removed)");
            }
        }

        /// <summary>
        /// Helper method: Activate user on a given connection
        /// </summary>
        private void DisconnectActiveSessionForUser(ITikConnection connection, string username)
        {
            _logger.LogInformation("   🔌 DisconnectActiveSessionForUser: user={username}", username);
            
            try
            {
                // Query /ip/hotspot/active for this user's sessions
                _logger.LogInformation("   🔍 Looking for active sessions for {username}...", username);
                
                var printCmd = connection.CreateCommand("/ip/hotspot/active/print");
                var results = SafeExecuteList(printCmd).ToList();
                
                if (results.Count == 0)
                {
                    _logger.LogInformation("   ℹ️ No active sessions found for {username}", username);
                    return;
                }

                // Look for sessions matching this username
                bool foundSession = false;
                foreach (var session in results)
                {
                    string sessionUsername = session.GetResponseField("user");
                    if (sessionUsername == username)
                    {
                        string sessionId = session.GetResponseField(".id");
                        _logger.LogInformation("   🗑️ Found active session ID {sessionId} for {username}, disconnecting...", sessionId, username);
                        
                        var removeCmd = connection.CreateCommand("/ip/hotspot/active/remove");
                        removeCmd.AddParameter(".id", sessionId);
                        SafeExecuteList(removeCmd);
                        
                        _logger.LogInformation("   ✅ Active session {sessionId} removed for {username}", sessionId, username);
                        foundSession = true;
                    }
                }

                if (!foundSession)
                {
                    _logger.LogInformation("   ℹ️ No active sessions found with username {username}", username);
                }
            }
            catch (Exception ex)
            {
                if (IsEmptyResponseException(ex))
                {
                    _logger.LogInformation("   ℹ️ No active sessions to disconnect (expected for inactive users)");
                }
                else
                {
                    _logger.LogWarning("   ⚠️ Error disconnecting active session: {message}", ex.Message);
                    throw;
                }
            }
        }

        private async Task ActivateUserOnConnection(ITikConnection connection, string username, int durationHours)
        {
            _logger.LogInformation("   📋 ActivateUserOnConnection: user={username}, duration={durationHours}h", username, durationHours);
            
            List<HotspotUser> users;
            try
            {
                _logger.LogInformation("   🔍 Looking up existing hotspot user: {username}", username);
                users = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).ToList();
                _logger.LogInformation("   ✅ User lookup complete: found {count} existing user(s)", users.Count);
            }
            catch (Exception ex)
            {
                if (IsEmptyResponseException(ex))
                {
                    _logger.LogInformation("   ℹ️ Empty response during user lookup (expected in some cases)");
                    users = new List<HotspotUser>();
                }
                else
                    throw;
            }

            var profile = $"profile-{durationHours}h";
            _logger.LogInformation("   ⏱️ Using profile: {profile}", profile);
            
            // Ensure the requested profile exists 
            await EnsureProfileExistsOnConnectionAsync(connection, profile).ConfigureAwait(false);
            
            // Execute set or add
            if (users.Count > 0)
            {
                var user = users.First();
                _logger.LogInformation("   ✏️ Updating existing user (ID: {userId})", user.Id);

                // IMPORTANT: Disconnect any active sessions for this user to reset the uptime counter
                // This is critical for reactivations of the same plan
                _logger.LogInformation("   🔌 Disconnecting any active sessions for user {username} to reset uptime counter", username);
                try
                {
                    DisconnectActiveSessionForUser(connection, username);
                    _logger.LogInformation("   ✅ Active sessions cleared for {username}", username);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("   ⚠️ Warning disconnecting active session: {message}", ex.Message);
                    // Don't fail here - this is not critical, continue with the update
                }

                _logger.LogInformation("   ♻️ Resetting permanent counters for user {username}", username);
                try
                {
                    var resetCmd = connection.CreateCommand("/ip/hotspot/user/reset-counters");
                    resetCmd.AddParameter("numbers", username);
                    resetCmd.ExecuteNonQuery();
                    _logger.LogInformation("   ✅ Counters reset successfully for {username}", username);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("   ⚠️ Could not reset counters for {username}: {message}", username, ex.Message);
                }

                var setCommand = connection.CreateCommand("/ip/hotspot/user/set");
                // Use .id parameter for set command (required by MikroTik API)
                setCommand.AddParameter(".id", user.Id);
                setCommand.AddParameter("profile", profile);
                setCommand.AddParameter("limit-uptime", $"{durationHours}h");
                setCommand.AddParameter("disabled", "no");

                try
                {
                    _logger.LogInformation("   ⚙️ Executing SET command for user {username}", username);
                    setCommand.ExecuteList();
                    _logger.LogInformation("   ✅ User updated successfully with new duration {durationHours}h", durationHours);
                }
                catch (Exception ex)
                {
                    if (!IsEmptyResponseException(ex))
                        throw;
                    _logger.LogInformation("   ℹ️ SET command returned empty response (expected)");
                }
            }
            else
            {
                _logger.LogInformation("   ➕ Creating new hotspot user: {username}", username);
                
                var addCommand = connection.CreateCommand("/ip/hotspot/user/add");
                addCommand.AddParameter("name", username);
                addCommand.AddParameter("password", username);
                addCommand.AddParameter("profile", profile);
                addCommand.AddParameter("limit-uptime", $"{durationHours}h");

                _logger.LogInformation("   ⚙️ Executing ADD command for user {username}", username);
                SafeExecuteList(addCommand);
                _logger.LogInformation("   ✅ User created successfully");
            }

            // Verification
            _logger.LogInformation("   🔍 Verifying user exists...");
            try
            {
                var verified = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).FirstOrDefault();
                if (verified == null)
                {
                    _logger.LogError("   ❌ Verification failed: user '{username}' not found after operation", username);
                    throw new InvalidOperationException($"Failed to verify activation: user '{username}' not found after operation");
                }
                _logger.LogInformation("   ✅ User verified successfully");
            }
            catch (Exception ex)
            {
                if (IsEmptyResponseException(ex))
                {
                    _logger.LogWarning("   ⚠️ Activation inconclusive: device returned '!empty' during verification");
                    throw new InvalidOperationException("Activation inconclusive: device returned '!empty' during verification.");
                }
                throw;
            }
        }

        private void BindMacOnConnection(ITikConnection connection, string macAddress, int durationHours)
        {
            Console.WriteLine($"   📌 BindMacOnConnection: mac={macAddress}, duration={durationHours}h");
            
            try
            {
                // Check if binding already exists using raw command
                Console.WriteLine("   🔍 Checking for existing MAC bindings...");
                var printCmd = connection.CreateCommand("/ip/hotspot/ip-binding/print");
                printCmd.AddParameter(".proplist", ".id");
                printCmd.AddParameter("?mac-address", macAddress);
                
                var results = SafeExecuteList(printCmd).ToList();
                Console.WriteLine($"   ✅ MAC binding check complete: found {results.Count} existing binding(s)");

                // Remove old binding if exists
                if (results.Count > 0)
                {
                    var bindingId = results[0].GetResponseField(".id");
                    Console.WriteLine($"   🗑️ Removing old binding (ID: {bindingId})...");
                    var removeCommand = connection.CreateCommand("/ip/hotspot/ip-binding/remove");
                    removeCommand.AddParameter(".id", bindingId);
                    Console.WriteLine("   ⚙️ Executing REMOVE command for old binding");
                    SafeExecuteList(removeCommand);
                    Console.WriteLine("   ✅ Old binding removed successfully");
                }

                // Add new binding with bypass type
                Console.WriteLine($"   ➕ Creating new binding: type=bypassed, timeout={durationHours}h");
                var addCommand = connection.CreateCommand("/ip/hotspot/ip-binding/add");
                addCommand.AddParameter("mac-address", macAddress);
                addCommand.AddParameter("type", "bypassed");
                if (durationHours > 0)
                {
                    addCommand.AddParameter("timeout", $"{durationHours}h");
                }

                Console.WriteLine("   ⚙️ Executing ADD command for new binding");
                SafeExecuteList(addCommand);
                Console.WriteLine("   ✅ New binding created successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"   ❌ MAC binding failed: {ex.Message}");
                throw new InvalidOperationException($"Failed to bind MAC address {macAddress} to bypass: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Helper method: Ensure profile exists on a given connection
        /// </summary>
        private async Task EnsureProfileExistsOnConnectionAsync(ITikConnection connection, string profileName)
        {
            if (connection == null || string.IsNullOrWhiteSpace(profileName))
                return;

            try
            {
                _logger.LogInformation("   🔍 Checking if profile exists: {profileName}", profileName);
                
                // Check if profile already exists
                var printCmd = connection.CreateCommand("/ip/hotspot/user/profile/print");
                printCmd.AddParameter("?name", profileName);
                var existingProfiles = SafeExecuteList(printCmd).ToList();
                
                if (existingProfiles.Count > 0)
                {
                    _logger.LogInformation("   ✅ Profile already exists: {profileName}", profileName);
                    return;
                }

                _logger.LogInformation("   ➕ Creating new profile: {profileName}", profileName);
                
                // Extract duration from profile name (e.g., "profile-2h" → 2 hours)
                int sessionTimeoutSeconds = 0;
                if (profileName.Contains("-"))
                {
                    var parts = profileName.Split('-');
                    if (parts.Length > 1 && int.TryParse(parts[parts.Length - 1].Replace("h", ""), out int hours))
                    {
                        sessionTimeoutSeconds = hours * 3600; // Convert hours to seconds
                        _logger.LogInformation("   ⏱️ Calculated session timeout: {hours}h ({seconds}s)", hours, sessionTimeoutSeconds);
                    }
                }

                var addProfile = connection.CreateCommand("/ip/hotspot/user/profile/add");
                addProfile.AddParameter("name", profileName);
                
                // Special handling for blocked profile
                if (profileName == "profile-blocked")
                {
                    addProfile.AddParameter("rate-limit", "0/0"); // Block all traffic
                    _logger.LogInformation("   🚫 Profile set to block all internet access");
                }
                else
                {
                    // Set session timeout on the profile for paid profiles
                    if (sessionTimeoutSeconds > 0)
                    {
                        addProfile.AddParameter("session-timeout", $"{sessionTimeoutSeconds}s");
                        _logger.LogInformation("   ⏱️ Profile session-timeout set to: {timeout}s", sessionTimeoutSeconds);
                    }
                }
                
                SafeExecuteList(addProfile);
                _logger.LogInformation("   ✅ Profile created successfully");

                await Task.Delay(1000).ConfigureAwait(false);

                if (!HotspotProfileExists(connection, profileName))
                {
                    _logger.LogError("   ❌ Profile {profileName} still not found after creation", profileName);
                    throw new InvalidOperationException($"Failed to verify that profile '{profileName}' exists after creation.");
                }
            }
            catch (Exception ex)
            {
                if (IsEmptyResponseException(ex))
                    return;

                var msg = ex.Message ?? string.Empty;
                if (msg.Contains("already") || msg.Contains("exists") || msg.Contains("duplicate"))
                    return;

                _logger.LogError("   ❌ Error ensuring profile exists: {error}", ex.Message);
                throw;
            }
        }

        private void EnsureProfileExistsOnConnection(ITikConnection connection, string profileName)
        {
            EnsureProfileExistsOnConnectionAsync(connection, profileName).GetAwaiter().GetResult();
        }

        private bool HotspotProfileExists(ITikConnection connection, string profileName)
        {
            if (connection == null || string.IsNullOrWhiteSpace(profileName))
                return false;

            var printCmd = connection.CreateCommand("/ip/hotspot/user/profile/print");
            printCmd.AddParameter("?name", profileName);
            var profiles = SafeExecuteList(printCmd).ToList();
            return profiles.Count > 0;
        }
    }
}
