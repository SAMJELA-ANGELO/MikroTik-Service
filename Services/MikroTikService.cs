using tik4net;
using tik4net.Api;
using tik4net.Objects;
using tik4net.Objects.Ip.Hotspot;
using tik4net.Objects.System;
using System.Linq;
using System.Collections.Generic;
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
        
        private readonly ILogger<MikrotikService> _logger;

        public MikrotikService(ILogger<MikrotikService> logger)
        {
            _logger = logger;
            _logger.LogInformation("🔧 MikrotikService initialized");
        }

        private ITikConnection Connect(string ipAddress)
        {
            _logger.LogInformation("📡 Attempting to connect to MikroTik: {ip}", ipAddress);
            try
            {
                var connection = ConnectionFactory.CreateConnection(TikConnectionType.Api);
                connection.Open(ipAddress, user, pass);
                _logger.LogInformation("✅ Connected to MikroTik {ip} successfully", ipAddress);
                return connection;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ Failed to connect to MikroTik {ip}: {message}", ipAddress, ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Connect to a specific router by IP address
        /// </summary>
        private ITikConnection ConnectToRouter(string routerIP)
        {
            _logger.LogInformation("📡 Attempting to connect to router IP: {routerIP}", routerIP);
            try
            {
                var connection = ConnectionFactory.CreateConnection(TikConnectionType.Api);
                connection.Open(routerIP, user, pass);
                _logger.LogInformation("✅ Connected to router {routerIP} successfully", routerIP);
                return connection;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ Failed to connect to router {routerIP}: {message}", routerIP, ex.Message);
                throw;
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

        public async Task ActivateUser(string username, int durationHours)
        {
            _logger.LogInformation("🔄 ActivateUser START (PARALLEL MODE): username={username}, durationHours={durationHours}", username, durationHours);

            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException("Username is required");

            var activateTasks = new List<Task<(string RouterName, bool Success, string? Error)>>();

            for (int i = 0; i < routerIPs.Length; i++)
            {
                string capturedIP = routerIPs[i];
                string capturedName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";
                activateTasks.Add(Task.Factory.StartNew(() => AttemptActivateUserOnRouter(capturedIP, capturedName, username, durationHours), TaskCreationOptions.LongRunning));
            }

            var errors = new List<string>();
            while (activateTasks.Any())
            {
                var finishedTask = await Task.WhenAny(activateTasks);
                activateTasks.Remove(finishedTask);

                try
                {
                    var result = await finishedTask;
                    if (result.Success)
                    {
                        _logger.LogInformation("✅ ActivateUser SUCCESS on {routerName} (PARALLEL)", result.RouterName);

                        if (activateTasks.Any())
                        {
                            _ = Task.WhenAll(activateTasks).ContinueWith(t =>
                            {
                                if (t.IsFaulted)
                                {
                                    _logger.LogWarning(t.Exception, "⚠️ Some background router activations failed, but primary succeeded.");
                                }
                            }, TaskContinuationOptions.OnlyOnFaulted);
                        }
                        return;
                    }

                    if (!string.IsNullOrWhiteSpace(result.Error))
                        errors.Add(result.Error);
                }
                catch (Exception ex)
                {
                    errors.Add(ex.Message);
                    _logger.LogWarning(ex, "⚠️ One router failed, checking next...");
                }
            }

            string allErrors = errors.Any() ? string.Join("; ", errors) : "No router returned a successful activation.";
            _logger.LogError("❌ Failed to activate {username} on any available router (parallel). Errors: {errors}", username, allErrors);
            throw new Exception($"Failed to activate {username} on any available router. Errors:\n{allErrors}");
        }

        private (string RouterName, bool Success, string? Error) AttemptActivateUserOnRouter(
            string routerIP, string routerName, string username, int durationHours)
        {
            _logger.LogInformation("🔄 [{routerName}] Attempting to activate user {username}...", routerName, username);
            try
            {
                using var connection = ConnectToRouter(routerIP);
                EnsureProfileExistsOnConnection(connection, $"profile-{durationHours}h");
                ActivateUserOnConnection(connection, username, durationHours);
                _logger.LogInformation("✅ [{routerName}] User {username} activated successfully", routerName, username);
                return (routerName, true, null);
            }
            catch (Exception ex)
            {
                string errorMsg = $"{routerName}: {ex.Message}";
                _logger.LogError(ex, "❌ [{routerName}] Activation failed: {message}", routerName, ex.Message);
                return (routerName, false, errorMsg);
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
                loginCmd.ExecuteNonQuery();
                Console.WriteLine("✅ User is now Active (forced login)");
            }
            catch (Exception ex)
            {
                if (!IsEmptyResponseException(ex))
                {
                    Console.WriteLine($"❌ Forced login failed: {ex.Message}");
                    throw;
                }
                Console.WriteLine("✅ Forced login completed with empty response (tik4net !empty case)");
            }
        }

        public async Task MoveHostToActive(string username, string password, string macAddress, string ip)
        {
            _logger.LogInformation("🔄 MoveHostToActive START (PARALLEL MODE): username={username}", username);

            if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password) ||
                string.IsNullOrWhiteSpace(macAddress) || string.IsNullOrWhiteSpace(ip))
            {
                throw new ArgumentException("Username, password, MAC address, and IP are required");
            }

            var moveTasks = new List<Task<(string RouterName, bool Success, string? Error)>>();
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string capturedIP = routerIPs[i];
                string capturedName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";
                moveTasks.Add(Task.Factory.StartNew(() => AttemptMoveHostToActiveOnRouter(capturedIP, capturedName, username, password, macAddress, ip), TaskCreationOptions.LongRunning));
            }

            var errors = new List<string>();
            while (moveTasks.Any())
            {
                var finishedTask = await Task.WhenAny(moveTasks);
                moveTasks.Remove(finishedTask);

                try
                {
                    var result = await finishedTask;
                    if (result.Success)
                    {
                        _logger.LogInformation("✅ MoveHostToActive SUCCESS on {routerName} (PARALLEL)", result.RouterName);

                        if (moveTasks.Any())
                        {
                            _ = Task.WhenAll(moveTasks).ContinueWith(t =>
                            {
                                if (t.IsFaulted)
                                {
                                    _logger.LogWarning(t.Exception, "⚠️ Some background router move attempts failed, but primary succeeded.");
                                }
                            }, TaskContinuationOptions.OnlyOnFaulted);
                        }
                        return;
                    }

                    if (!string.IsNullOrWhiteSpace(result.Error))
                        errors.Add(result.Error);
                }
                catch (Exception ex)
                {
                    errors.Add(ex.Message);
                    _logger.LogWarning(ex, "⚠️ One router failed, checking next...");
                }
            }

            string allErrors = errors.Any() ? string.Join("; ", errors) : "No router returned a successful move.";
            _logger.LogError("❌ Failed to move host to active for {username} on any available router (parallel). Errors: {errors}", username, allErrors);
            throw new Exception($"Failed to move host to active for {username} on any available router. Errors:\n{allErrors}");
        }

        private (string RouterName, bool Success, string? Error) AttemptMoveHostToActiveOnRouter(
            string routerIP, string routerName, string username, string password, string macAddress, string ip)
        {
            _logger.LogInformation("🔄 [{routerName}] Attempting to move host to active for {username}...", routerName, username);
            try
            {
                using var connection = ConnectToRouter(routerIP);
                MoveHostToActive(connection, username, password, macAddress, ip);
                _logger.LogInformation("✅ [{routerName}] Host moved to active for {username}", routerName, username);
                return (routerName, true, null);
            }
            catch (Exception ex)
            {
                string errorMsg = $"{routerName}: {ex.Message}";
                _logger.LogError(ex, "❌ [{routerName}] Failed to move host to active for {username}: {message}", routerName, username, ex.Message);
                return (routerName, false, errorMsg);
            }
        }

        public async Task DeactivateUser(string username)
        {
            _logger.LogInformation("🔄 DeactivateUser START (PARALLEL MODE): username={username}", username);
            
            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException("Username is required");

            // Create tasks for each router (parallel attempts)
            var deactivateTasks = new List<Task<(string RouterName, bool Success, string? Error)>>();
            
            for (int i = 0; i < routerIPs.Length; i++)
            {
                // CRITICAL: Capture in local variables to avoid closure bug
                string capturedIP = routerIPs[i];
                string capturedName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";
                
                // Create a task for this router
                var task = Task.Factory.StartNew(() => AttemptDeactivateUserOnRouter(capturedIP, capturedName, username), TaskCreationOptions.LongRunning);
                deactivateTasks.Add(task);
            }

            // Try routers in parallel using WhenAny - first one to succeed wins
            var errors = new List<string>();
            while (deactivateTasks.Any())
            {
                var finishedTask = await Task.WhenAny(deactivateTasks);
                deactivateTasks.Remove(finishedTask);

                try
                {
                    var result = await finishedTask;
                    if (result.Success)
                    {
                        _logger.LogInformation("✅ DeactivateUser SUCCESS on {routerName} (PARALLEL)", result.RouterName);
                        
                        if (deactivateTasks.Any())
                        {
                            _ = Task.WhenAll(deactivateTasks).ContinueWith(t =>
                            {
                                if (t.IsFaulted)
                                {
                                    _logger.LogWarning(t.Exception, "⚠️ Some background router deactivations failed, but primary succeeded.");
                                }
                            }, TaskContinuationOptions.OnlyOnFaulted);
                        }
                        return;
                    }

                    if (!string.IsNullOrWhiteSpace(result.Error))
                        errors.Add(result.Error);
                }
                catch (Exception ex)
                {
                    errors.Add(ex.Message);
                    _logger.LogWarning(ex, "⚠️ One router failed, checking next...");
                }
            }

            string allErrors = errors.Any() ? string.Join("; ", errors) : "No router returned a successful deactivation.";
            _logger.LogError("❌ Failed to deactivate {username} on any available router (parallel). Errors: {errors}", username, allErrors);
            throw new Exception($"Failed to deactivate {username} on any available router. Errors:\n{allErrors}");
        }

        /// <summary>
        /// Helper method: Attempt to deactivate user on a single router
        /// </summary>
        private (string RouterName, bool Success, string? Error) AttemptDeactivateUserOnRouter(
            string routerIP, string routerName, string username)
        {
            _logger.LogInformation("🔄 [{routerName}] Attempting to deactivate user {username}...", routerName, username);

            try
            {
                using var connection = ConnectToRouter(routerIP);

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
                        return (routerName, true, null);
                    }
                    catch (Exception ex)
                    {
                        if (IsEmptyResponseException(ex))
                        {
                            _logger.LogInformation("✅ [{routerName}] User {username} deactivation verified (empty response)", routerName, username);
                            return (routerName, true, null);
                        }
                        throw;
                    }
                }
                else
                {
                    _logger.LogWarning("⚠️ [{routerName}] User {username} not found", routerName, username);
                    return (routerName, false, $"User {username} not found on {routerName}");
                }
            }
            catch (Exception ex)
            {
                string errorMsg = $"{routerName}: {ex.Message}";
                _logger.LogError(ex, "❌ [{routerName}] Failed to deactivate user {username}: {message}", routerName, username, ex.Message);
                return (routerName, false, errorMsg);
            }
        }

        public async Task CreateUser(string username, string password)
        {
            _logger.LogInformation("🟢 CreateUser START (PARALLEL MODE): username={username}", username);

            if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
            {
                throw new ArgumentException("Username and password are required");
            }

            var createTasks = new List<Task<(string RouterName, bool Success, string? Error)>>();
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string capturedIP = routerIPs[i];
                string capturedName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";
                createTasks.Add(Task.Factory.StartNew(() => AttemptCreateUserOnRouter(capturedIP, capturedName, username, password), TaskCreationOptions.LongRunning));
            }

            var errors = new List<string>();
            while (createTasks.Any())
            {
                var finishedTask = await Task.WhenAny(createTasks);
                createTasks.Remove(finishedTask);

                try
                {
                    var result = await finishedTask;
                    if (result.Success)
                    {
                        _logger.LogInformation("✅ CreateUser SUCCESS on {routerName} (PARALLEL)", result.RouterName);

                        if (createTasks.Any())
                        {
                            _ = Task.WhenAll(createTasks).ContinueWith(t =>
                            {
                                if (t.IsFaulted)
                                {
                                    _logger.LogWarning(t.Exception, "⚠️ Some background router create attempts failed, but primary succeeded.");
                                }
                            }, TaskContinuationOptions.OnlyOnFaulted);
                        }
                        return;
                    }

                    if (!string.IsNullOrWhiteSpace(result.Error))
                        errors.Add(result.Error);
                }
                catch (Exception ex)
                {
                    errors.Add(ex.Message);
                    _logger.LogWarning(ex, "⚠️ One router failed, checking next...");
                }
            }

            string allErrors = errors.Any() ? string.Join("; ", errors) : "No router returned a successful create.";
            _logger.LogError("❌ Failed to create {username} on any available router (parallel). Errors: {errors}", username, allErrors);
            throw new Exception($"Failed to create user {username} on any available router. Errors:\n{allErrors}");
        }

        private (string RouterName, bool Success, string? Error) AttemptCreateUserOnRouter(
            string routerIP, string routerName, string username, string password)
        {
            _logger.LogInformation("🔄 [{routerName}] Attempting to create user {username}...", routerName, username);
            try
            {
                using var connection = ConnectToRouter(routerIP);
                EnsureProfileExistsOnConnection(connection, "profile-blocked");

                var addCommand = connection.CreateCommand("/ip/hotspot/user/add");
                addCommand.AddParameter("name", username);
                addCommand.AddParameter("password", password);
                addCommand.AddParameter("profile", "profile-blocked");
                addCommand.AddParameter("disabled", "yes");

                try
                {
                    addCommand.ExecuteNonQuery();
                    _logger.LogInformation("✅ [{routerName}] User {username} created with blocked access and disabled until activation", routerName, username);
                }
                catch (Exception ex)
                {
                    if (!IsEmptyResponseException(ex))
                        throw;
                    _logger.LogInformation("   ℹ️ User creation returned empty response, proceeding to verification");
                }

                try
                {
                    var created = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).FirstOrDefault();
                    if (created == null)
                        throw new InvalidOperationException($"Failed to verify creation of user '{username}'");

                    _logger.LogInformation("✅ [{routerName}] User {username} creation verified", routerName, username);
                    return (routerName, true, null);
                }
                catch (Exception ex)
                {
                    if (IsEmptyResponseException(ex))
                    {
                        _logger.LogInformation("✅ [{routerName}] User {username} creation verified (empty response)", routerName, username);
                        return (routerName, true, null);
                    }
                    throw;
                }
            }
            catch (Exception ex)
            {
                string errorMsg = $"{routerName}: {ex.Message}";
                _logger.LogError(ex, "❌ [{routerName}] Failed to create user {username}: {message}", routerName, username, ex.Message);
                return (routerName, false, errorMsg);
            }
        }

        public async Task DeleteUser(string username)
        {
            _logger.LogInformation("🗑️ DeleteUser START (PARALLEL MODE): username={username}", username);
            
            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException("Username is required");

            // Create tasks for each router (parallel attempts)
            var deleteTasks = new List<Task<(string RouterName, bool Success, string? Error)>>();
            
            for (int i = 0; i < routerIPs.Length; i++)
            {
                // CRITICAL: Capture in local variables to avoid closure bug
                string capturedIP = routerIPs[i];
                string capturedName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";
                
                // Create a task for this router
                var task = Task.Factory.StartNew(() => AttemptDeleteUserOnRouter(capturedIP, capturedName, username), TaskCreationOptions.LongRunning);
                deleteTasks.Add(task);
            }

            // Try routers in parallel using WhenAny - first one to succeed wins
            var errors = new List<string>();
            while (deleteTasks.Any())
            {
                var finishedTask = await Task.WhenAny(deleteTasks);
                deleteTasks.Remove(finishedTask);

                try
                {
                    var result = await finishedTask;
                    if (result.Success)
                    {
                        _logger.LogInformation("✅ DeleteUser SUCCESS on {routerName} (PARALLEL)", result.RouterName);
                        
                        if (deleteTasks.Any())
                        {
                            _ = Task.WhenAll(deleteTasks).ContinueWith(t =>
                            {
                                if (t.IsFaulted)
                                {
                                    _logger.LogWarning(t.Exception, "⚠️ Some background router deletions failed, but primary succeeded.");
                                }
                            }, TaskContinuationOptions.OnlyOnFaulted);
                        }
                        return;
                    }

                    if (!string.IsNullOrWhiteSpace(result.Error))
                        errors.Add(result.Error);
                }
                catch (Exception ex)
                {
                    errors.Add(ex.Message);
                    _logger.LogWarning(ex, "⚠️ One router failed, checking next...");
                }
            }

            string allErrors = errors.Any() ? string.Join("; ", errors) : "No router returned a successful deletion.";
            _logger.LogError("❌ Failed to delete {username} on any available router (parallel). Errors: {errors}", username, allErrors);
            throw new Exception($"Failed to delete {username} on any available router. Errors:\n{allErrors}");
        }

        /// <summary>
        /// Helper method: Attempt to delete user on a single router
        /// </summary>
        private (string RouterName, bool Success, string? Error) AttemptDeleteUserOnRouter(
            string routerIP, string routerName, string username)
        {
            _logger.LogInformation("🔄 [{routerName}] Attempting to delete user {username}...", routerName, username);

            try
            {
                using var connection = ConnectToRouter(routerIP);

                var user = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).FirstOrDefault();
                if (user == null)
                {
                    _logger.LogWarning("⚠️ [{routerName}] User {username} not found", routerName, username);
                    return (routerName, false, $"User {username} not found on {routerName}");
                }

                var removeCommand = connection.CreateCommand("/ip/hotspot/user/remove");
                removeCommand.AddParameter(".id", user.Id);
                removeCommand.ExecuteNonQuery();

                _logger.LogInformation("✅ [{routerName}] User {username} deleted successfully", routerName, username);
                return (routerName, true, null);
            }
            catch (Exception ex)
            {
                if (IsEmptyResponseException(ex))
                {
                    _logger.LogInformation("✅ [{routerName}] User {username} deletion verified (empty response)", routerName, username);
                    return (routerName, true, null);
                }
                
                string errorMsg = $"{routerName}: {ex.Message}";
                _logger.LogError(ex, "❌ [{routerName}] Failed to delete user {username}: {message}", routerName, username, ex.Message);
                return (routerName, false, errorMsg);
            }
        }

        public async Task DisableUser(string username)
        {
            _logger.LogInformation("🔒 DisableUser START (PARALLEL MODE): username={username}", username);
            
            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException("Username is required");

            // Create tasks for each router (parallel attempts)
            var disableTasks = new List<Task<(string RouterName, bool Success, string? Error)>>();
            
            for (int i = 0; i < routerIPs.Length; i++)
            {
                // CRITICAL: Capture in local variables to avoid closure bug
                string capturedIP = routerIPs[i];
                string capturedName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";
                
                // Create a task for this router
                var task = Task.Factory.StartNew(() => AttemptDisableUserOnRouter(capturedIP, capturedName, username), TaskCreationOptions.LongRunning);
                disableTasks.Add(task);
            }

            // Try routers in parallel using WhenAny - first one to succeed wins
            var errors = new List<string>();
            while (disableTasks.Any())
            {
                var finishedTask = await Task.WhenAny(disableTasks);
                disableTasks.Remove(finishedTask);

                try
                {
                    var result = await finishedTask;
                    if (result.Success)
                    {
                        _logger.LogInformation("✅ DisableUser SUCCESS on {routerName} (PARALLEL)", result.RouterName);
                        
                        if (disableTasks.Any())
                        {
                            _ = Task.WhenAll(disableTasks).ContinueWith(t =>
                            {
                                if (t.IsFaulted)
                                {
                                    _logger.LogWarning(t.Exception, "⚠️ Some background router disables failed, but primary succeeded.");
                                }
                            }, TaskContinuationOptions.OnlyOnFaulted);
                        }
                        return;
                    }

                    if (!string.IsNullOrWhiteSpace(result.Error))
                        errors.Add(result.Error);
                }
                catch (Exception ex)
                {
                    errors.Add(ex.Message);
                    _logger.LogWarning(ex, "⚠️ One router failed, checking next...");
                }
            }

            string allErrors = errors.Any() ? string.Join("; ", errors) : "No router returned a successful disable.";
            _logger.LogError("❌ Failed to disable {username} on any available router (parallel). Errors: {errors}", username, allErrors);
            throw new Exception($"Failed to disable {username} on any available router. Errors:\n{allErrors}");
        }

        /// <summary>
        /// Helper method: Attempt to disable user on a single router
        /// </summary>
        private (string RouterName, bool Success, string? Error) AttemptDisableUserOnRouter(
            string routerIP, string routerName, string username)
        {
            _logger.LogInformation("🔄 [{routerName}] Attempting to disable user {username}...", routerName, username);

            try
            {
                using var connection = ConnectToRouter(routerIP);

                var users = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).ToList();

                if (users.Count == 0)
                {
                    _logger.LogWarning("⚠️ [{routerName}] User {username} not found", routerName, username);
                    return (routerName, false, $"User {username} not found on {routerName}");
                }

                var user = users.First();
                var setCommand = connection.CreateCommand("/ip/hotspot/user/set");
                setCommand.AddParameter(".id", user.Id);
                setCommand.AddParameter("disabled", "yes");

                try
                {
                    setCommand.ExecuteNonQuery();
                    _logger.LogInformation("✅ [{routerName}] User {username} disabled successfully", routerName, username);
                }
                catch (Exception ex)
                {
                    if (!IsEmptyResponseException(ex))
                        throw;
                }

                // Verify the user is now disabled
                var verified = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).FirstOrDefault();
                if (verified != null)
                {
                    _logger.LogInformation("✅ [{routerName}] User {username} disable verified", routerName, username);
                    return (routerName, true, null);
                }
                
                return (routerName, false, "Disable verification failed");
            }
            catch (Exception ex)
            {
                if (IsEmptyResponseException(ex))
                {
                    _logger.LogInformation("✅ [{routerName}] User {username} disable verified (empty response)", routerName, username);
                    return (routerName, true, null);
                }
                
                string errorMsg = $"{routerName}: {ex.Message}";
                _logger.LogError(ex, "❌ [{routerName}] Failed to disable user {username}: {message}", routerName, username, ex.Message);
                return (routerName, false, errorMsg);
            }
        }

        public async Task<object> TestConnection()
        {
            var testTasks = new List<Task<(string RouterName, bool Success, string? Identity)>>();
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string capturedIP = routerIPs[i];
                string capturedName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";
                var task = Task.Factory.StartNew(() => 
                {
                    try {
                        using var conn = ConnectToRouter(capturedIP);
                        var id = conn.LoadAll<SystemIdentity>().FirstOrDefault();
                        return (capturedName, true, id?.Name);
                    } catch (Exception ex) {
                        _logger.LogWarning(ex, "⚠️ [{routerName}] Test failed", capturedName);
                        return (capturedName, false, (string?)null);
                    }
                }, TaskCreationOptions.LongRunning);
                testTasks.Add(task);
            }
            while (testTasks.Any())
            {
                var finished = await Task.WhenAny(testTasks);
                testTasks.Remove(finished);
                var result = await finished;
                if (result.Success)
                {
                    _logger.LogInformation("✅ TestConnection SUCCESS on {routerName}", result.RouterName);
                    return new { connected = true, identity = result.Identity, router = result.RouterName };
                }
            }
            return new { connected = false, error = "No routers available" };
        }

        public async Task<List<HotspotUser>> ListHotspotUsers()
        {
            var listTasks = new List<Task<(string RouterName, List<HotspotUser> Users)>>();
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string capturedIP = routerIPs[i];
                string capturedName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";
                var task = Task.Factory.StartNew(() => 
                {
                    try {
                        using var conn = ConnectToRouter(capturedIP);
                        var users = conn.LoadAll<HotspotUser>().ToList();
                        return (capturedName, users);
                    } catch (Exception ex) {
                        _logger.LogWarning(ex, "⚠️ [{routerName}] List failed", capturedName);
                        return (capturedName, new List<HotspotUser>());
                    }
                }, TaskCreationOptions.LongRunning);
                listTasks.Add(task);
            }
            while (listTasks.Any())
            {
                var finished = await Task.WhenAny(listTasks);
                listTasks.Remove(finished);
                var result = await finished;
                if (result.Users.Count > 0)
                {
                    _logger.LogInformation("✅ ListHotspotUsers SUCCESS on {routerName}", result.RouterName);
                    return result.Users;
                }
            }
            throw new Exception("Failed to list hotspot users from any available router.");
        }

        public async Task<HotspotUser> GetUserDetails(string username)
        {
            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException("Username is required");

            var detailsTasks = new List<Task<(string RouterName, HotspotUser User)>>();
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string capturedIP = routerIPs[i];
                string capturedName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";
                var task = Task.Factory.StartNew(() => 
                {
                    try {
                        using var conn = ConnectToRouter(capturedIP);
                        var user = conn.LoadList<HotspotUser>(conn.CreateParameter("name", username)).FirstOrDefault();
#pragma warning disable CS8603
                        return (capturedName, user);
#pragma warning restore CS8603
                    } catch (Exception ex) {
                        _logger.LogWarning(ex, "⚠️ [{routerName}] Details query failed", capturedName);
                        return (capturedName, (HotspotUser)null);
                    }
                }, TaskCreationOptions.LongRunning);
                detailsTasks.Add(task);
            }
            while (detailsTasks.Any())
            {
                var finished = await Task.WhenAny(detailsTasks);
                detailsTasks.Remove(finished);
                var result = await finished;
                if (result.User != null)
                {
                    _logger.LogInformation("✅ GetUserDetails SUCCESS on {routerName}", result.RouterName);
                    return result.User;
                }
            }
            return null;
        }

        public async Task<List<HotspotActive>> GetActiveUsers()
        {
            var activeTasks = new List<Task<(string RouterName, List<HotspotActive> Users)>>();
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string capturedIP = routerIPs[i];
                string capturedName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";
                var task = Task.Factory.StartNew(() => 
                {
                    try {
                        using var conn = ConnectToRouter(capturedIP);
                        var users = conn.LoadAll<HotspotActive>().ToList();
                        return (capturedName, users);
                    } catch (Exception ex) {
                        _logger.LogWarning(ex, "⚠️ [{routerName}] Active query failed", capturedName);
                        return (capturedName, new List<HotspotActive>());
                    }
                }, TaskCreationOptions.LongRunning);
                activeTasks.Add(task);
            }
            while (activeTasks.Any())
            {
                var finished = await Task.WhenAny(activeTasks);
                activeTasks.Remove(finished);
                var result = await finished;
                if (result.Users.Count > 0)
                {
                    _logger.LogInformation("✅ GetActiveUsers SUCCESS on {routerName}", result.RouterName);
                    return result.Users;
                }
            }
            throw new Exception("Failed to get active users from any available router.");
        }

        public async Task BindMacToBypass(string macAddress, int durationHours)
        {
            if (string.IsNullOrWhiteSpace(macAddress))
                throw new ArgumentException("MAC address is required");

            var bindingTasks = new List<Task<(string RouterName, bool Success, string? Error)>>();
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string capturedIP = routerIPs[i];
                string capturedName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";
                var task = Task.Factory.StartNew(() => 
                {
                    try {
                        using var conn = ConnectToRouter(capturedIP);
                        var existing = conn.LoadList<dynamic>(conn.CreateParameter("mac-address", macAddress)).ToList();
                        if (existing?.Count > 0) {
                            var rmCmd = conn.CreateCommand("/ip/hotspot/ip-binding/remove");
                            rmCmd.AddParameter(".id", existing[0].Id);
                            try { rmCmd.ExecuteNonQuery(); }
                            catch (Exception ex) { if (!IsEmptyResponseException(ex)) throw; }
                        }
                        var addCmd = conn.CreateCommand("/ip/hotspot/ip-binding/add");
                        addCmd.AddParameter("mac-address", macAddress);
                        addCmd.AddParameter("type", "bypassed");
                        if (durationHours > 0) addCmd.AddParameter("timeout", $"{durationHours}h");
                        try { addCmd.ExecuteNonQuery(); }
                        catch (Exception ex) { if (!IsEmptyResponseException(ex)) throw; }
                        return (capturedName, true, (string?)null);
                    } catch (Exception ex) { return (capturedName, false, ex.Message); }
                }, TaskCreationOptions.LongRunning);
                bindingTasks.Add(task);
            }

            var errors = new List<string>();
            while (bindingTasks.Any())
            {
                var finishedTask = await Task.WhenAny(bindingTasks);
                bindingTasks.Remove(finishedTask);
                try {
                    var result = await finishedTask;
                    if (result.Success) {
                        _logger.LogInformation("✅ BindMacToBypass SUCCESS on {routerName}", result.RouterName);
                        if (bindingTasks.Any())
                            _ = Task.WhenAll(bindingTasks).ContinueWith(t => {
                                if (t.IsFaulted) _logger.LogWarning(t.Exception, "⚠️ Background bind failed");
                            }, TaskContinuationOptions.OnlyOnFaulted);
                        return;
                    }
                    if (result.Error != null) errors.Add(result.Error);
                } catch (Exception ex) { errors.Add(ex.Message); }
            }
            throw new Exception($"Failed to bind MAC {macAddress}: {string.Join("; ", errors)}");
        }

        public async Task UnbindMac(string macAddress)
        {
            if (string.IsNullOrWhiteSpace(macAddress))
                throw new ArgumentException("MAC address is required");

            var unbindingTasks = new List<Task<(string RouterName, bool Success, string? Error)>>();
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string capturedIP = routerIPs[i];
                string capturedName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";
                var task = Task.Factory.StartNew(() => 
                {
                    try {
                        using var conn = ConnectToRouter(capturedIP);
                        var bindings = conn.LoadList<dynamic>(conn.CreateParameter("mac-address", macAddress)).ToList();
                        if (bindings?.Count > 0) {
                            var rmCmd = conn.CreateCommand("/ip/hotspot/ip-binding/remove");
                            rmCmd.AddParameter(".id", bindings[0].Id);
                            try { rmCmd.ExecuteNonQuery(); }
                            catch (Exception ex) { if (!IsEmptyResponseException(ex)) throw; }
                            return (capturedName, true, (string?)null);
                        }
                        return (capturedName, false, $"MAC {macAddress} not found");
                    } catch (Exception ex) { return (capturedName, false, ex.Message); }
                }, TaskCreationOptions.LongRunning);
                unbindingTasks.Add(task);
            }

            var errors = new List<string>();
            while (unbindingTasks.Any())
            {
                var finishedTask = await Task.WhenAny(unbindingTasks);
                unbindingTasks.Remove(finishedTask);
                try {
                    var result = await finishedTask;
                    if (result.Success) {
                        _logger.LogInformation("✅ UnbindMac SUCCESS on {routerName}", result.RouterName);
                        if (unbindingTasks.Any())
                            _ = Task.WhenAll(unbindingTasks).ContinueWith(t => {
                                if (t.IsFaulted) _logger.LogWarning(t.Exception, "⚠️ Background unbind failed");
                            }, TaskContinuationOptions.OnlyOnFaulted);
                        return;
                    }
                    if (result.Error != null) errors.Add(result.Error);
                } catch (Exception ex) { errors.Add(ex.Message); }
            }
            throw new Exception($"Failed to unbind MAC {macAddress}: {string.Join("; ", errors)}");
        }

        /// <summary>
        /// FAILOVER METHOD: Tries to activate a user on the first available router (Home then School)
        /// Used for the "mobile Starlink" scenario where only one router is online at a time
        /// </summary>
        public async Task<string> ActivateOnAvailableRouter(string username, int durationHours, string? macAddress = null)
        {
            _logger.LogInformation("🚀 ActivateOnAvailableRouter START (PARALLEL MODE): username={username}, duration={durationHours}h, mac={macAddress}", username, durationHours, macAddress ?? "null");
            
            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException("Username is required");

            // Create tasks for each router (parallel attempts)
            var activationTasks = new List<Task<(string RouterName, string RouterIP, bool Success, string? Error)>>();
            for (int i = 0; i < routerIPs.Length; i++)
            {
                // CRITICAL: Capture in local variables to avoid closure bug
                // Must capture by value, not reference, so each task gets its own copy
                string capturedIP = routerIPs[i];
                string capturedName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";
                
                var task = Task.Factory.StartNew(() => AttemptActivateOnRouter(capturedIP, capturedName, username, durationHours, macAddress), TaskCreationOptions.LongRunning);
                activationTasks.Add(task);
            }

            var errors = new List<string>();
            while (activationTasks.Any())
            {
                var finishedTask = await Task.WhenAny(activationTasks);
                activationTasks.Remove(finishedTask);

                try
                {
                    var result = await finishedTask;
                    if (result.Success)
                    {
                        _logger.LogInformation("✅ ActivateOnAvailableRouter SUCCESS on {routerName} (PARALLEL)", result.RouterName);

                        if (activationTasks.Any())
                        {
                            _ = Task.WhenAll(activationTasks).ContinueWith(t =>
                            {
                                if (t.IsFaulted)
                                {
                                    _logger.LogWarning(t.Exception, "⚠️ Some background router activations failed, but the user is already active on one router.");
                                }
                            }, TaskContinuationOptions.OnlyOnFaulted);
                        }

                        return result.RouterName;
                    }

                    if (!string.IsNullOrWhiteSpace(result.Error))
                        errors.Add(result.Error);
                }
                catch (Exception ex)
                {
                    errors.Add(ex.Message);
                    _logger.LogWarning(ex, "⚠️ One router failed during activation, checking the next available router.");
                }
            }

            string allErrors = errors.Any() ? string.Join("; ", errors) : "No router returned a successful activation.";
            _logger.LogError("❌ Failed to activate {username} on any available router (parallel). Errors: {errors}", username, allErrors);
            throw new Exception($"Failed to activate {username} on any available router. Errors:\n{allErrors}");
        }

        /// <summary>
        /// Helper method: Attempt to activate on a single router
        /// </summary>
        private (string RouterName, string RouterIP, bool Success, string? Error) AttemptActivateOnRouter(
            string routerIP, string routerName, string username, int durationHours, string? macAddress)
        {
            _logger.LogInformation("🔄 [{routerName}] Attempting to activate {username}...", routerName, username);

            try
            {
                using var connection = ConnectToRouter(routerIP);
                _logger.LogInformation("✅ [{routerName}] Connected successfully", routerName);
                
                // Try to activate the user
                _logger.LogInformation("   Step 1/2: Creating/updating hotspot user on {routerName}...", routerName);
                ActivateUserOnConnection(connection, username, durationHours);
                _logger.LogInformation("   ✅ Hotspot user ready on {routerName}", routerName);
                
                // If MAC address provided, also bind it
                if (!string.IsNullOrWhiteSpace(macAddress))
                {
                    try
                    {
                        _logger.LogInformation("   Step 2/2: Binding MAC address {macAddress} on {routerName}...", macAddress, routerName);
                        BindMacOnConnection(connection, macAddress, durationHours);
                        _logger.LogInformation("   ✅ MAC address bound successfully on {routerName}", routerName);
                    }
                    catch (Exception macError)
                    {
                        _logger.LogWarning("⚠️ MAC binding warning on {routerName} (non-critical): {message}", routerName, macError.Message);
                        // Don't fail activation if MAC binding fails
                    }
                }

                _logger.LogInformation("✅ [{routerName}] Successfully activated {username}", routerName, username);
                return (routerName, routerIP, true, null);
            }
            catch (Exception ex)
            {
                string errorMsg = $"{routerName} ({routerIP}): {ex.Message}";
                _logger.LogError(ex, "❌ [{routerName}] Activation failed: {message}", routerName, ex.Message);
                return (routerName, routerIP, false, errorMsg);
            }
        }

        /// <summary>
        /// GIFT FLOW METHOD: Creates hotspot user WITHOUT instant MAC binding
        /// Recipient will log in manually; MAC will be captured on first login
        /// Used when one user buys a bundle for another user
        /// </summary>
        public string CreateHotspotUserOnly(string username, int durationHours)
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
                    
                    using var connection = ConnectToRouter(routerIP);
                    
                    // Try to activate the user (creates hotspot account with limit-uptime)
                    ActivateUserOnConnection(connection, username, durationHours);
                    
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
                    using var connection = ConnectToRouter(routerIP);
                    Console.WriteLine($"✅ Connected to {routerName}");

                    // 1. Create/Update the Hotspot User first (so they exist in the DB)
                    Console.WriteLine($"   1️⃣ Creating/updating hotspot user...");
                    ActivateUserOnConnection(connection, username, durationHours);
                    Console.WriteLine($"   ✅ Hotspot user ready");

                    // 2. FORCE the login session for this specific device
                    // This makes the MikroTik move the user from 'Hosts' to 'Active'
                    Console.WriteLine($"   2️⃣ Forcing login session (user: {username}, mac: {mac}, ip: {ip})...");
                    await MoveHostToActive(username, password, mac, ip);
                    Console.WriteLine($"   ✅ Login session forced successfully by MoveHostToActive");

                    // If we get here, login was successful
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
            _logger.LogInformation("📌 BindMacOnAvailableRouter START (PARALLEL MODE): mac={macAddress}, duration={durationHours}h", macAddress, durationHours);
            
            if (string.IsNullOrWhiteSpace(macAddress))
                throw new ArgumentException("MAC address is required");

            // Create tasks for each router (parallel attempts)
            var bindingTasks = new List<Task<(string RouterName, string RouterIP, bool Success, string? Error)>>();
            
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";
                
                // Create a task for this router that will attempt binding
                var task = Task.Factory.StartNew(() => AttemptBindMacOnRouter(routerIP, routerName, macAddress, durationHours), TaskCreationOptions.LongRunning);
                bindingTasks.Add(task);
            }

            // Try routers in parallel using WhenAny - first one to succeed wins
            try
            {
                var completedTask = Task.WhenAny(bindingTasks.Cast<Task>()).Result;
                
                // Find which task completed successfully
                foreach (var task in bindingTasks)
                {
                    if (task.IsCompleted && !task.IsFaulted)
                    {
                        var result = task.Result;
                        if (result.Success)
                        {
                            _logger.LogInformation("✅ BindMacOnAvailableRouter SUCCESS on {routerName} (PARALLEL)", result.RouterName);
                            return result.RouterName;
                        }
                    }
                }

                // If no task succeeded, throw aggregated error
                var errors = bindingTasks
                    .Where(t => t.IsCompleted)
                    .Select(t => t.IsFaulted ? t.Exception?.Message : t.Result.Error)
                    .Where(e => !string.IsNullOrEmpty(e))
                    .ToList();

                string allErrors = string.Join("; ", errors);
                _logger.LogError("❌ Failed to bind MAC {macAddress} on any available router (parallel). Errors: {errors}", macAddress, allErrors);
                throw new Exception($"Failed to bind MAC {macAddress} on any available router. Errors:\n{allErrors}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ Parallel MAC binding failed: {message}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Helper method: Attempt to bind MAC on a single router
        /// </summary>
        private (string RouterName, string RouterIP, bool Success, string? Error) AttemptBindMacOnRouter(
            string routerIP, string routerName, string macAddress, int durationHours)
        {
            _logger.LogInformation("🔄 [{routerName}] Attempting to bind MAC {macAddress}...", routerName, macAddress);

            try
            {
                using var connection = ConnectToRouter(routerIP);
                _logger.LogInformation("✅ [{routerName}] Connected successfully", routerName);
                
                // Try to bind the MAC
                BindMacOnConnection(connection, macAddress, durationHours);
                
                _logger.LogInformation("✅ [{routerName}] Successfully bound MAC {macAddress}", routerName, macAddress);
                return (routerName, routerIP, true, null);
            }
            catch (Exception ex)
            {
                string errorMsg = $"{routerName} ({routerIP}): {ex.Message}";
                _logger.LogError(ex, "❌ [{routerName}] MAC binding failed: {message}", routerName, ex.Message);
                return (routerName, routerIP, false, errorMsg);
            }
        }

        /// <summary>
        /// FAILOVER METHOD: Tries to unbind MAC on Available routers (tries both Home and School) - PARALLEL MODE
        /// Used when session expires - MAC might exist on either router
        /// </summary>
        public void UnbindMacOnAvailableRouters(string macAddress)
        {
            if (string.IsNullOrWhiteSpace(macAddress))
                throw new ArgumentException("MAC address is required");

            _logger.LogInformation("🔄 UnbindMacOnAvailableRouters START (PARALLEL MODE): mac={macAddress}", macAddress);

            // Create tasks for each router (parallel attempts)
            var unbindTasks = new List<Task<(string RouterName, bool Found, bool Success, string? Error)>>();
            
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";
                
                // Create a task for this router that will attempt unbinding
                var task = Task.Factory.StartNew(() => AttemptUnbindMacOnRouter(routerIP, routerName, macAddress), TaskCreationOptions.LongRunning);
                unbindTasks.Add(task);
            }

            // Try all routers in parallel
            try
            {
                Task.WaitAll(unbindTasks.Cast<Task>().ToArray());
                
                // Check if any router found and removed the binding
                var foundOnAnyRouter = unbindTasks
                    .Where(t => t.IsCompleted && !t.IsFaulted)
                    .Any(t => t.Result.Found && t.Result.Success);

                if (!foundOnAnyRouter)
                {
                    _logger.LogWarning("ℹ️ MAC {macAddress} not found on any router (may already be removed)", macAddress);
                }
                else
                {
                    _logger.LogInformation("✅ UnbindMacOnAvailableRouters completed (PARALLEL MODE)");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ Parallel unbind failed: {message}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Helper method: Attempt to unbind MAC on a single router
        /// </summary>
        private (string RouterName, bool Found, bool Success, string? Error) AttemptUnbindMacOnRouter(
            string routerIP, string routerName, string macAddress)
        {
            _logger.LogInformation("🔄 [{routerName}] Searching for MAC binding...", routerName);

            try
            {
                using var connection = ConnectToRouter(routerIP);
                
                // Try to find and remove the binding using raw command instead of LoadList
                // (LoadList fails due to HotspotIpBinding not having TikEntityAttribute)
                var printCmd = connection.CreateCommand("/ip/hotspot/ip-binding/print");
                printCmd.AddParameter(".proplist", ".id");
                printCmd.AddParameter("?mac-address", macAddress);
                
                var bindings = SafeExecuteList(printCmd).ToList();
                _logger.LogInformation("   Found {count} binding(s) for MAC {macAddress} on {routerName}", bindings.Count, macAddress, routerName);

                if (bindings != null && bindings.Count > 0)
                {
                    var bindingId = bindings[0].GetResponseField(".id");
                    var removeCommand = connection.CreateCommand("/ip/hotspot/ip-binding/remove");
                    removeCommand.AddParameter(".id", bindingId);

                    try
                    {
                        _logger.LogInformation("   Executing REMOVE command for binding (ID: {id}) on {routerName}", bindingId, routerName);
                        removeCommand.ExecuteNonQuery();
                        _logger.LogInformation("   ✅ Removed MAC binding from {routerName}", routerName);
                        return (routerName, true, true, null);
                    }
                    catch (Exception ex)
                    {
                        if (!IsEmptyResponseException(ex))
                            throw;
                        _logger.LogInformation("   Binding removed with empty response on {routerName}", routerName);
                        return (routerName, true, true, null);
                    }
                }
                else
                {
                    _logger.LogInformation("   ℹ️ MAC not found on {routerName}", routerName);
                    return (routerName, false, true, null);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "⚠️ [{routerName}] Could not check: {message}", routerName, ex.Message);
                return (routerName, false, false, ex.Message);
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
                        removeCmd.ExecuteNonQuery();
                        
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

        private void ActivateUserOnConnection(ITikConnection connection, string username, int durationHours)
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
            // EnsureProfileExistsOnConnection(connection, profile);
            
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
                setCommand.AddParameter(".id", user.Id);
                setCommand.AddParameter("profile", profile);
                setCommand.AddParameter("limit-uptime", $"{durationHours}h");
                setCommand.AddParameter("disabled", "no");

                try
                {
                    _logger.LogInformation("   ⚙️ Executing SET command for user {username}", username);
                    setCommand.ExecuteNonQuery();
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

                try
                {
                    _logger.LogInformation("   ⚙️ Executing ADD command for user {username}", username);
                    addCommand.ExecuteNonQuery();
                    _logger.LogInformation("   ✅ User created successfully");
                }
                catch (Exception ex)
                {
                    if (!IsEmptyResponseException(ex))
                        throw;
                    _logger.LogInformation("   ℹ️ ADD command returned empty response (expected)");
                }
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
                    try
                    {
                        Console.WriteLine("   ⚙️ Executing REMOVE command for old binding");
                        removeCommand.ExecuteNonQuery();
                        Console.WriteLine("   ✅ Old binding removed successfully");
                    }
                    catch (Exception ex)
                    {
                        if (!IsEmptyResponseException(ex))
                            throw;
                        Console.WriteLine("   ℹ️ Old binding removed with empty response");
                    }
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

                try
                {
                    Console.WriteLine("   ⚙️ Executing ADD command for new binding");
                    addCommand.ExecuteNonQuery();
                    Console.WriteLine("   ✅ New binding created successfully");
                }
                catch (Exception ex)
                {
                    if (!IsEmptyResponseException(ex))
                        throw;
                    Console.WriteLine("   ℹ️ New binding created with empty response");
                }
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
        private void EnsureProfileExistsOnConnection(ITikConnection connection, string profileName)
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
                
                addProfile.ExecuteNonQuery();
                _logger.LogInformation("   ✅ Profile created successfully");

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

