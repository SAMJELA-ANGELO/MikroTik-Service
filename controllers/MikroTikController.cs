using Microsoft.AspNetCore.Mvc;
using MikrotikService.Services;
using tik4net.Objects.Ip.Hotspot;

namespace MikrotikService.Controllers
{
    [ApiController]
    [Route("api/mikrotik")]
    public class MikrotikController : ControllerBase
    {
        private readonly Services.MikrotikService _service;

        public MikrotikController(Services.MikrotikService service)
        {
            _service = service;
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
            if (string.IsNullOrWhiteSpace(dto.Username))
            {
                return BadRequest(new { message = "Username is required" });
            }

            try
            {
                string useRouter = _service.ActivateOnAvailableRouter(dto.Username, dto.DurationHours, dto.MacAddress);
                return Ok(new { 
                    message = $"User {dto.Username} activated for {dto.DurationHours} hours",
                    activeRouter = useRouter,
                    macBound = !string.IsNullOrWhiteSpace(dto.MacAddress)
                });
            }
            catch (Exception ex)
            {
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
            if (string.IsNullOrWhiteSpace(dto.Username))
            {
                return BadRequest(new { message = "Username is required" });
            }

            try
            {
                string activeRouter = _service.CreateHotspotUserOnly(dto.Username, dto.DurationHours);
                return Ok(new { 
                    message = $"Hotspot user {dto.Username} created for {dto.DurationHours} hours (gift - no MAC binding)",
                    activeRouter = activeRouter,
                    note = "User will log in manually; MAC binding will happen on first login"
                });
            }
            catch (Exception ex)
            {
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
            if (string.IsNullOrWhiteSpace(dto.MacAddress))
            {
                return BadRequest(new { message = "MAC address is required" });
            }

            try
            {
                string activeRouter = _service.BindMacOnAvailableRouter(dto.MacAddress, dto.DurationHours ?? 0);
                return Ok(new { 
                    message = $"MAC address {dto.MacAddress} bound to bypass list",
                    activeRouter = activeRouter
                });
            }
            catch (Exception ex)
            {
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
                _service.UnbindMacOnAvailableRouters(macAddress);
                return Ok(new { message = $"MAC address {macAddress} search and remove completed on all routers" });
            }
            catch (Exception ex)
            {
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
            if (string.IsNullOrWhiteSpace(dto.Username) || string.IsNullOrWhiteSpace(dto.Password) ||
                string.IsNullOrWhiteSpace(dto.MacAddress) || string.IsNullOrWhiteSpace(dto.IpAddress))
            {
                return BadRequest(new { message = "Username, password, MAC address, and IP address are required" });
            }

            try
            {
                string activeRouter = _service.SilentLogin(dto.Username, dto.Password, dto.MacAddress, dto.IpAddress, dto.DurationHours);
                return Ok(new { 
                    message = $"Silent login successful for {dto.Username}",
                    activeRouter = activeRouter,
                    note = "User is now actively connected to the hotspot"
                });
            }
            catch (Exception ex)
            {
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