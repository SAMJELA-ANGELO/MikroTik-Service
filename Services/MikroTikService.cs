using tik4net;
using tik4net.Api;
using tik4net.Objects;
using tik4net.Objects.Ip.Hotspot;
using tik4net.Objects.System;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace MikrotikService.Services
{
    public class MikrotikService
    {
        private readonly string host = "10.0.0.2";
        private readonly string user = "admin";
        private readonly string pass = "To2day";
        
        // Multi-router support (for failover)
        private readonly string[] routerIPs = new[] { "10.0.0.2", "10.0.0.3", "192.168.1.154" }; // Home: 10.0.0.2, School: 10.0.0.3, Local: 192.168.1.154
        private readonly string[] routerNames = new[] { "Home", "School", "Local" }; // Router names for logging
        
        private readonly ILogger<MikrotikService> _logger;

        public MikrotikService(ILogger<MikrotikService> logger)
        {
            _logger = logger;
            _logger.LogInformation("🔧 MikrotikService initialized");
        }

        private ITikConnection Connect()
        {
            _logger.LogInformation("📡 Attempting to connect to MikroTik (default: {host})", host);
            try
            {
                var connection = ConnectionFactory.CreateConnection(TikConnectionType.Api);
                connection.Open(host, user, pass);
                _logger.LogInformation("✅ Connected to MikroTik {host} successfully", host);
                return connection;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ Failed to connect to MikroTik {host}: {message}", host, ex.Message);
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

        public void ActivateUser(string username, int durationHours)
        {
            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException("Username is required");

            using var connection = Connect();

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
            EnsureProfileExistsOnConnection(connection, profile);
            
            if (users.Count == 0)
            {
                throw new InvalidOperationException($"User '{username}' not found. Create the user first before activating.");
            }

            var user = users.First();

            var setCommand = connection.CreateCommand("/ip/hotspot/user/set");
            setCommand.AddParameter(".id", user.Id);
            setCommand.AddParameter("profile", profile);
            setCommand.AddParameter("limit-uptime", $"{durationHours}h");
            setCommand.AddParameter("disabled", "no");

            try
            {
                setCommand.ExecuteNonQuery();
                _logger.LogInformation("✅ User {username} activated with {hours}h access", username, durationHours);
            }
            catch (Exception ex)
            {
                if (!IsEmptyResponseException(ex))
                    throw;
                // else continue to verification
            }

            // Verification: ensure the user now exists on the device
            try
            {
                var verified = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).FirstOrDefault();
                if (verified == null)
                {
                    throw new InvalidOperationException($"Failed to verify activation: user '{username}' not found after operation");
                }
            }
            catch (Exception ex)
            {
                if (IsEmptyResponseException(ex))
                {
                    throw new InvalidOperationException("Activation inconclusive: device returned '!empty' during verification. Check device state manually.");
                }
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

        public void MoveHostToActive(string username, string password, string macAddress, string ip)
        {
            using var connection = Connect();
            MoveHostToActive(connection, username, password, macAddress, ip);
        }

        public void DeactivateUser(string username)
        {
            using var connection = Connect();

            var users = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).ToList();

            if (users.Count > 0)
            {
                var user = users.First();
                var removeCommand = connection.CreateCommand("/ip/hotspot/user/remove");
                removeCommand.AddParameter(".id", user.Id);

                try
                {
                    removeCommand.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    if (IsEmptyResponseException(ex))
                    {
                        return;
                    }
                    throw;
                }
            }
        }

        public void CreateUser(string username, string password)
        {
            if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
            {
                throw new ArgumentException("Username and password are required");
            }
            using var connection = Connect();

            // Ensure the blocked profile exists
            EnsureProfileExistsOnConnection(connection, "profile-blocked");

            var addCommand = connection.CreateCommand("/ip/hotspot/user/add");
            addCommand.AddParameter("name", username);
            addCommand.AddParameter("password", password);
            addCommand.AddParameter("profile", "profile-blocked");
            addCommand.AddParameter("disabled", "no");

            // Try to add the user; if tik4net throws the known '!empty' response error,
            // continue to verification step instead of failing immediately.
            try
            {
                addCommand.ExecuteNonQuery();
                _logger.LogInformation("✅ User {username} created with blocked access", username);
            }
            catch (Exception ex)
            {
                if (!IsEmptyResponseException(ex))
                    throw;
                // fall through to verification
            }

            // Verify the user exists. If verification fails with '!empty', consider it success.
            try
            {
                var created = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).FirstOrDefault();
                if (created == null)
                {
                    throw new InvalidOperationException($"Failed to verify creation of user '{username}'");
                }
            }
            catch (Exception ex)
            {
                if (IsEmptyResponseException(ex))
                {
                    return; // assume success when device responds with '!empty'
                }
                throw;
            }
        }

        public void DeleteUser(string username)
        {
            try
            {
                using var connection = Connect();

                var user = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).FirstOrDefault();
                if (user == null)
                {
                    throw new KeyNotFoundException($"User '{username}' not found");
                }

                var removeCommand = connection.CreateCommand("/ip/hotspot/user/remove");
                removeCommand.AddParameter(".id", user.Id);
                removeCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                if (IsEmptyResponseException(ex))
                {
                    return;
                }
                throw;
            }
        }

        public void DisableUser(string username)
        {
            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException("Username is required");

            try
            {
                using var connection = Connect();

                var users = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).ToList();

                if (users.Count == 0)
                {
                    throw new KeyNotFoundException($"User '{username}' not found");
                }

                var user = users.First();
                var setCommand = connection.CreateCommand("/ip/hotspot/user/set");
                setCommand.AddParameter(".id", user.Id);
                setCommand.AddParameter("disabled", "yes");

                try
                {
                    setCommand.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    if (!IsEmptyResponseException(ex))
                        throw;
                }

                // Verify the user is now disabled
                var verified = connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).FirstOrDefault();
                if (verified == null)
                {
                    throw new InvalidOperationException($"Failed to verify disabling of user '{username}'");
                }
            }
            catch (Exception ex)
            {
                if (IsEmptyResponseException(ex))
                {
                    return;
                }
                throw;
            }
        }

        public object TestConnection()
        {
            using var connection = Connect();
            var identity = connection.LoadAll<SystemIdentity>().First();
            return new { connected = true, identity = identity.Name };
        }

        public List<HotspotUser> ListHotspotUsers()
        {
            using var connection = Connect();
            return connection.LoadAll<HotspotUser>().ToList();
        }

        public HotspotUser GetUserDetails(string username)
        {
#pragma warning disable CS8603
            using var connection = Connect();
            return connection.LoadList<HotspotUser>(connection.CreateParameter("name", username)).FirstOrDefault();
#pragma warning restore CS8603
        }

        public List<HotspotActive> GetActiveUsers()
        {
            using var connection = Connect();
            return connection.LoadAll<HotspotActive>().ToList();
        }

        public void BindMacToBypass(string macAddress, int durationHours)
        {
            if (string.IsNullOrWhiteSpace(macAddress))
                throw new ArgumentException("MAC address is required");

            using var connection = Connect();

            try
            {
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
                }
                catch (Exception ex)
                {
                    if (!IsEmptyResponseException(ex))
                        throw;
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to bind MAC address {macAddress} to bypass: {ex.Message}", ex);
            }
        }

        public void UnbindMac(string macAddress)
        {
            if (string.IsNullOrWhiteSpace(macAddress))
                throw new ArgumentException("MAC address is required");

            using var connection = Connect();

            try
            {
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
                    }
                    catch (Exception ex)
                    {
                        if (!IsEmptyResponseException(ex))
                            throw;
                    }
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to unbind MAC address {macAddress}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// FAILOVER METHOD: Tries to activate a user on the first available router (Home then School)
        /// Used for the "mobile Starlink" scenario where only one router is online at a time
        /// </summary>
        public string ActivateOnAvailableRouter(string username, int durationHours, string? macAddress = null)
        {
            _logger.LogInformation("🚀 ActivateOnAvailableRouter START: username={username}, duration={durationHours}h, mac={macAddress}", username, durationHours, macAddress ?? "null");
            
            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException("Username is required");

            List<string> errors = new List<string>();
            string? successfulRouter = null;

            // Try each router in order
            for (int i = 0; i < routerIPs.Length; i++)
            {
                string routerIP = routerIPs[i];
                string routerName = routerNames.Length > i ? routerNames[i] : $"Router-{i}";

                _logger.LogInformation("🔄 [{routerName}] Attempting to activate {username}...", routerName, username);

                try
                {
                    using var connection = ConnectToRouter(routerIP);
                    _logger.LogInformation("✅ [{routerName}] Connected successfully", routerName);
                    
                    // Try to activate the user
                    _logger.LogInformation("   Step 1/2: Creating/updating hotspot user...");
                    ActivateUserOnConnection(connection, username, durationHours);
                    _logger.LogInformation("   ✅ Hotspot user ready");
                    
                    // If we get here, connection was successful
                    successfulRouter = routerName;
                    _logger.LogInformation("✅ [{routerName}] Successfully activated {username}", routerName, username);

                    // If MAC address provided, also bind it
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
                            // Don't fail activation if MAC binding fails
                        }
                    }

                    _logger.LogInformation("🚀 ActivateOnAvailableRouter SUCCESS on {routerName}", routerName);
                    return successfulRouter;
                }
                catch (Exception ex)
                {
                    string errorMsg = $"{routerName} ({routerIP}): {ex.Message}";
                    errors.Add(errorMsg);
                    _logger.LogError(ex, "❌ [{routerName}] Activation failed: {message}", routerName, ex.Message);
                    // Continue to next router
                }
            }

            // If we get here, all routers failed
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
        public string SilentLogin(string username, string password, string mac, string ip, int durationHours)
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
                    MoveHostToActive(username, password, mac, ip);
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
                    
                    using var connection = ConnectToRouter(routerIP);
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
                    using var connection = ConnectToRouter(routerIP);
                    
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
            EnsureProfileExistsOnConnection(connection, profile);
            
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
    }
}
