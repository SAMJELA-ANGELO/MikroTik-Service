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
    }

    public class ActivateDto
    {
        public string? Username { get; set; }
        public int DurationHours { get; set; }
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
}