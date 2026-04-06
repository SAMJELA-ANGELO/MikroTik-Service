using Microsoft.AspNetCore.Mvc;
using MikrotikService.Services;
using tik4net.Objects.Ip.Hotspot;
using Microsoft.Extensions.Logging;

namespace MikrotikService.Controllers
{
    [ApiController]
    [Route("api/mikrotik")]
    public class MikrotikController : ControllerBase
    {
        private readonly Services.MikrotikService _service;
        private readonly ILogger<MikrotikController> _logger;

        public MikrotikController(Services.MikrotikService service, ILogger<MikrotikController> logger)
        {
            _service = service;
            _logger = logger;
        }

        [HttpGet("test-connection")]
        public IActionResult TestConnection()
        {
            try
            {
                var result = _service.TestConnection();
                return Ok(new { message = "MikroTik connection successful", data = result });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { message = "Connection failed", error = ex.Message });
            }
        }

        [HttpGet("users")]
        public IActionResult ListUsers()
        {
            try
            {
                var users = _service.ListHotspotUsers();
                return Ok(new { users });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { message = "Failed to list users", error = ex.Message });
            }
        }

        [HttpGet("users/{username}")]
        public IActionResult GetUserDetails(string username)
        {
            try
            {
                var user = _service.GetUserDetails(username);
                if (user == null)
                {
                    return NotFound(new { message = $"User {username} not found" });
                }
                return Ok(new { user });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { message = "Failed to get user details", error = ex.Message });
            }
        }

        [HttpGet("active-users")]
        public IActionResult GetActiveUsers()
        {
            try
            {
                var activeUsers = _service.GetActiveUsers();
                return Ok(new { activeUsers });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { message = "Failed to get active users", error = ex.Message });
            }
        }

        [HttpPost("users")]
        public IActionResult Create([FromBody] CreateUserDto dto)
        {
            if (string.IsNullOrWhiteSpace(dto.Username) || string.IsNullOrWhiteSpace(dto.Password))
            {
                return BadRequest(new { message = "Username and password are required" });
            }

            try
            {
                _service.CreateUser(dto.Username, dto.Password);
                return Ok(new { message = $"User {dto.Username} created" });
            }
            catch (InvalidOperationException ex)
            {
                return Conflict(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { message = "Failed to create user", error = ex.Message });
            }
        }

        [HttpPost("disable")]
        public IActionResult Disable([FromBody] DisableDto dto)
        {
            if (dto.Username == null)
            {
                return BadRequest("Username is required");
            }
            try
            {
                _service.DisableUser(dto.Username);
                return Ok(new { message = $"User {dto.Username} disabled (account kept)" });
            }
            catch (KeyNotFoundException ex)
            {
                return NotFound(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { message = "Failed to disable user", error = ex.Message });
            }
        }

        [HttpDelete("delete")]
        public IActionResult DeleteUser(string username)
        {
            if (string.IsNullOrWhiteSpace(username))
            {
                return BadRequest(new { message = "Username is required" });
            }

            try
            {
                _service.DeleteUser(username);
                return Ok(new { message = $"User {username} permanently deleted" });
            }
            catch (KeyNotFoundException ex)
            {
                return NotFound(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { message = "Failed to delete user", error = ex.Message });
            }
        }

        [HttpDelete("users/{username}")]
        public IActionResult Delete(string username)
        {
            if (string.IsNullOrWhiteSpace(username))
            {
                return BadRequest(new { message = "Username is required" });
            }

            try
            {
                _service.DeleteUser(username);
                return Ok(new { message = $"User {username} deleted" });
            }
            catch (KeyNotFoundException ex)
            {
                return NotFound(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { message = "Failed to delete user", error = ex.Message });
            }
        }

        [HttpPost("activate")]
        public IActionResult Activate([FromBody] ActivateDto dto)
        {
            if (dto.Username == null)
            {
                return BadRequest("Username is required");
            }
            try
            {
                _service.ActivateUser(dto.Username, dto.DurationHours);
                return Ok(new { message = $"User {dto.Username} activated for {dto.DurationHours} hours" });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { message = "Failed to activate user", error = ex.Message });
            }
        }

        [HttpDelete("deactivate")]
        public IActionResult Deactivate([FromBody] DeactivateDto dto)
        {
            if (dto.Username == null)
            {
                return BadRequest("Username is required");
            }
            try
            {
                _service.DeactivateUser(dto.Username);
                return Ok(new { message = $"User {dto.Username} deactivated" });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { message = "Failed to deactivate user", error = ex.Message });
            }
        }

        [HttpPost("bind-mac")]
        public IActionResult BindMac([FromBody] BindMacDto dto)
        {
            if (string.IsNullOrWhiteSpace(dto.MacAddress))
            {
                return BadRequest(new { message = "MAC address is required" });
            }

            try
            {
                _service.BindMacToBypass(dto.MacAddress, dto.DurationHours ?? 0);
                return Ok(new { message = $"MAC address {dto.MacAddress} bound to bypass list" });
            }
            catch (ArgumentException ex)
            {
                return BadRequest(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { message = "Failed to bind MAC address", error = ex.Message });
            }
        }

        [HttpDelete("unbind-mac")]
        public IActionResult UnbindMac([FromQuery] string macAddress)
        {
            if (string.IsNullOrWhiteSpace(macAddress))
            {
                return BadRequest(new { message = "MAC address is required" });
            }

            try
            {
                _service.UnbindMac(macAddress);
                return Ok(new { message = $"MAC address {macAddress} removed from bypass list" });
            }
            catch (ArgumentException ex)
            {
                return BadRequest(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { message = "Failed to unbind MAC address", error = ex.Message });
            }
        }

        /// <summary>
        /// FAILOVER ENDPOINT: Tries to activate user on first available router (Home then School)
        /// </summary>
        [HttpPost("activate-failover")]
        public IActionResult ActivateFailover([FromBody] ActivateDto dto)
        {
            _logger.LogInformation("📥 [ActivateFailover] Endpoint called: username={username}, durationHours={durationHours}, mac={mac}", 
                dto.Username, dto.DurationHours, dto.MacAddress ?? "null");
            
            if (string.IsNullOrWhiteSpace(dto.Username))
            {
                _logger.LogWarning("⚠️ [ActivateFailover] Missing username");
                return BadRequest(new { message = "Username is required" });
            }

            try
            {
                // FIX: Decode the MAC address if provided - it may arrive URL-encoded (02%3A38... instead of 02:38...)
                string? decodedMac = null;
                if (!string.IsNullOrWhiteSpace(dto.MacAddress)) {
                    decodedMac = System.Net.WebUtility.UrlDecode(dto.MacAddress);
                    _logger.LogInformation("🔍 [ActivateFailover] Raw MAC: {rawMac} → Decoded MAC: {decodedMac}", 
                        dto.MacAddress, decodedMac);
                }
                
                _logger.LogInformation("🚀 [ActivateFailover] Calling service.ActivateOnAvailableRouter...");
                string useRouter = _service.ActivateOnAvailableRouter(dto.Username, dto.DurationHours, decodedMac);
                _logger.LogInformation("✅ [ActivateFailover] Success on router: {router}", useRouter);
                
                return Ok(new { 
                    message = $"User {dto.Username} activated for {dto.DurationHours} hours",
                    activeRouter = useRouter,
                    macBound = !string.IsNullOrWhiteSpace(decodedMac)
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ [ActivateFailover] Error: {message}", ex.Message);
                return StatusCode(503, new { 
                    message = "No MikroTik routers currently available", 
                    error = ex.Message,
                    hint = "Check if both Home (10.0.0.2) and School (10.0.0.3) routers are offline or VPN disconnected"
                });
            }
        }

        /// <summary>
        /// GIFT FLOW ENDPOINT: Create hotspot user WITHOUT instant MAC bypass
        /// Recipient will log in manually; MAC will be captured on first login
        /// </summary>
        [HttpPost("create-hotspot-user")]
        public IActionResult CreateHotspotUserOnly([FromBody] CreateHotspotUserDto dto)
        {
            _logger.LogInformation("📥 [CreateHotspotUserOnly] Endpoint called: username={username}, durationHours={durationHours}", 
                dto.Username, dto.DurationHours);
            
            if (string.IsNullOrWhiteSpace(dto.Username))
            {
                _logger.LogWarning("⚠️ [CreateHotspotUserOnly] Missing username");
                return BadRequest(new { message = "Username is required" });
            }

            try
            {
                _logger.LogInformation("🎁 [CreateHotspotUserOnly] Calling service.CreateHotspotUserOnly...");
                string activeRouter = _service.CreateHotspotUserOnly(dto.Username, dto.DurationHours);
                _logger.LogInformation("✅ [CreateHotspotUserOnly] Success on router: {router}", activeRouter);
                
                return Ok(new { 
                    message = $"Hotspot user {dto.Username} created for {dto.DurationHours} hours (gift - no MAC binding)",
                    activeRouter = activeRouter,
                    note = "User will log in manually; MAC binding will happen on first login"
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ [CreateHotspotUserOnly] Error: {message}", ex.Message);
                return StatusCode(503, new { 
                    message = "No MikroTik routers currently available", 
                    error = ex.Message,
                    hint = "Check if both Home (10.0.0.2) and School (10.0.0.3) routers are offline or VPN disconnected"
                });
            }
        }

        /// <summary>
        /// FAILOVER ENDPOINT: Tries to bind MAC on first available router (Home then School)
        /// </summary>
        [HttpPost("bind-mac-failover")]
        public IActionResult BindMacFailover([FromBody] BindMacDto dto)
        {
            _logger.LogInformation("📥 [BindMacFailover] Endpoint called: mac={mac}, durationHours={durationHours}", 
                dto.MacAddress, dto.DurationHours ?? 0);
            
            if (string.IsNullOrWhiteSpace(dto.MacAddress))
            {
                _logger.LogWarning("⚠️ [BindMacFailover] Missing MAC address");
                return BadRequest(new { message = "MAC address is required" });
            }

            try
            {
                // FIX: Decode the MAC address - it may arrive URL-encoded (02%3A38... instead of 02:38...)
                string decodedMac = System.Net.WebUtility.UrlDecode(dto.MacAddress);
                _logger.LogInformation("🔍 [BindMacFailover] Raw MAC: {rawMac} → Decoded MAC: {decodedMac}", 
                    dto.MacAddress, decodedMac);
                
                _logger.LogInformation("📌 [BindMacFailover] Calling service.BindMacOnAvailableRouter...");
                string activeRouter = _service.BindMacOnAvailableRouter(decodedMac, dto.DurationHours ?? 0);
                _logger.LogInformation("✅ [BindMacFailover] Success on router: {router}", activeRouter);
                
                return Ok(new { 
                    message = $"MAC address {dto.MacAddress} bound to bypass list",
                    activeRouter = activeRouter
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ [BindMacFailover] Error: {message}", ex.Message);
                return StatusCode(503, new { 
                    message = "No MikroTik routers currently available", 
                    error = ex.Message 
                });
            }
        }

        /// <summary>
        /// FAILOVER ENDPOINT: Unbind MAC from available routers (checks both Home and School)
        /// Used when session expires
        /// </summary>
        [HttpDelete("unbind-mac-failover")]
        public IActionResult UnbindMacFailover([FromQuery] string macAddress)
        {
            if (string.IsNullOrWhiteSpace(macAddress))
            {
                return BadRequest(new { message = "MAC address is required" });
            }

            try
            {
                // FIX: Decode the MAC address - it may arrive URL-encoded (02%3A38... instead of 02:38...)
                string decodedMac = System.Net.WebUtility.UrlDecode(macAddress);
                _logger.LogInformation("🔍 [UnbindMacFailover] Raw MAC: {rawMac} → Decoded MAC: {decodedMac}", 
                    macAddress, decodedMac);
                
                _service.UnbindMacOnAvailableRouters(decodedMac);
                _logger.LogInformation("✅ [UnbindMacFailover] Unbind completed for MAC: {decodedMac}", decodedMac);
                return Ok(new { message = $"MAC address {decodedMac} search and remove completed on all routers" });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ [UnbindMacFailover] Error: {message}", ex.Message);
                return StatusCode(500, new { message = "Error during unbind process", error = ex.Message });
            }
        }

        /// <summary>
        /// SILENT LOGIN ENDPOINT: Forces an active login session for automatic authentication
        /// This moves the user from 'Hosts' to 'Active' on the MikroTik hotspot
        /// Used for automatic authentication after payment or web login
        /// </summary>
        [HttpPost("silent-login")]
        public IActionResult SilentLogin([FromBody] SilentLoginDto dto)
        {
            _logger.LogInformation("📥 [SilentLogin] Endpoint called: username={username}, mac={mac}, ip={ip}", 
                dto.Username, dto.MacAddress, dto.IpAddress);
            
            if (string.IsNullOrWhiteSpace(dto.Username) || string.IsNullOrWhiteSpace(dto.Password) ||
                string.IsNullOrWhiteSpace(dto.MacAddress) || string.IsNullOrWhiteSpace(dto.IpAddress))
            {
                _logger.LogWarning("⚠️ [SilentLogin] Missing required parameters");
                return BadRequest(new { message = "Username, password, MAC address, and IP address are required" });
            }

            try
            {
                // FIX: Decode the MAC address - it may arrive URL-encoded (02%3A38... instead of 02:38...)
                string decodedMac = System.Net.WebUtility.UrlDecode(dto.MacAddress);
                _logger.LogInformation("🔍 [SilentLogin] Raw MAC: {rawMac} → Decoded MAC: {decodedMac}", 
                    dto.MacAddress, decodedMac);
                
                _logger.LogInformation("🔐 [SilentLogin] Calling service.SilentLogin...");
                string activeRouter = _service.SilentLogin(dto.Username, dto.Password, decodedMac, dto.IpAddress, dto.DurationHours);
                _logger.LogInformation("✅ [SilentLogin] Success on router: {router}", activeRouter);
                
                return Ok(new { 
                    message = $"Silent login successful for {dto.Username}",
                    activeRouter = activeRouter,
                    note = "User is now actively connected to the hotspot"
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ [SilentLogin] Error: {message}", ex.Message);
                return StatusCode(503, new { 
                    message = "Silent login failed on all MikroTik routers", 
                    error = ex.Message,
                    hint = "Check if both Home (10.0.0.2) and School (10.0.0.3) routers are offline or VPN disconnected"
                });
            }
        }
    }

    public class ActivateDto
    {
        public string? Username { get; set; }
        public int DurationHours { get; set; }
        public string? MacAddress { get; set; }
    }

    public class DeactivateDto
    {
        public string? Username { get; set; }
    }

    public class DisableDto
    {
        public string? Username { get; set; }
    }

    public class CreateUserDto
    {
        public string? Username { get; set; }
        public string? Password { get; set; }
        public int DurationHours { get; set; }
        public string? Profile { get; set; }
    }

    public class BindMacDto
    {
        public string? MacAddress { get; set; }
        public int? DurationHours { get; set; }
    }

    public class CreateHotspotUserDto
    {
        public string? Username { get; set; }
        public int DurationHours { get; set; }
    }

    public class SilentLoginDto
    {
        public string? Username { get; set; }
        public string? Password { get; set; }
        public string? MacAddress { get; set; }
        public string? IpAddress { get; set; }
        public int DurationHours { get; set; }
    }
}