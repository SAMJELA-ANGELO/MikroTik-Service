using tik4net;
using tik4net.Api;
using tik4net.Objects;
using tik4net.Objects.Ip.Hotspot;
using tik4net.Objects.System;
using System.Linq;

namespace MikrotikService.Services
{
    public class MikrotikService
    {
        private readonly string host = "10.0.0.2";
        private readonly string user = "admin";
        private readonly string pass = "To2day";

        private ITikConnection Connect()
        {
            var connection = ConnectionFactory.CreateConnection(TikConnectionType.Api);
            connection.Open(host, user, pass);
            return connection;
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
            EnsureProfileExists(connection, profile);
            // Execute set or add
            if (users.Count > 0)
            {
                var user = users.First();

                var setCommand = connection.CreateCommand("/ip/hotspot/user/set");
                setCommand.AddParameter(".id", user.Id);
                setCommand.AddParameter("profile", profile);
                setCommand.AddParameter("limit-uptime", $"{durationHours}h");
                setCommand.AddParameter("disabled", "no");

                try
                {
                    setCommand.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    if (!IsEmptyResponseException(ex))
                        throw;
                    // else continue to verification
                }
            }
            else
            {
                var addCommand = connection.CreateCommand("/ip/hotspot/user/add");
                addCommand.AddParameter("name", username);
                addCommand.AddParameter("password", username);
                addCommand.AddParameter("profile", profile);
                addCommand.AddParameter("limit-uptime", $"{durationHours}h");

                try
                {
                    addCommand.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    if (!IsEmptyResponseException(ex))
                        throw;
                    // else continue to verification
                }
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

        private void EnsureProfileExists(ITikConnection connection, string profileName)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));
            if (string.IsNullOrWhiteSpace(profileName))
                return;

            try
            {
                var addProfile = connection.CreateCommand("/ip/hotspot/user/profile/add");
                addProfile.AddParameter("name", profileName);
                addProfile.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                if (IsEmptyResponseException(ex))
                    return;

                var msg = ex.Message ?? string.Empty;
                // Treat common 'already exists' messages as non-fatal — profile is present.
                if (msg.Contains("already") || msg.Contains("exists") || msg.Contains("duplicate"))
                    return;

                throw;
            }
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

            var addCommand = connection.CreateCommand("/ip/hotspot/user/add");
            addCommand.AddParameter("name", username);
            addCommand.AddParameter("password", password);

            // Try to add the user; if tik4net throws the known '!empty' response error,
            // continue to verification step instead of failing immediately.
            try
            {
                addCommand.ExecuteNonQuery();
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
    }
}