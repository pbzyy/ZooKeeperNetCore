using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace ZooKeeperNetCoreTest
{
    public static class HttpRequestHelper
    {
        private static readonly HttpClient HttpClient = new HttpClient();

        /// <summary>
        /// GetAsync
        /// </summary>
        /// <param name="url">url</param>
        /// <returns>Task string</returns>
        public static async Task<string> DoGetAsync(string url)
        {
            return await HttpClient.GetStringAsync(url).ConfigureAwait(false);
        }

        /// <summary>
        /// PostAsync
        /// </summary>
        /// <param name="url">url</param>
        /// <param name="content">content</param>
        /// <param name="contentType">contentType</param>
        /// <returns>Task string</returns>
        public static async Task<string> DoPostAsync(string url, string content,
            string contentType = "application/x-www-form-urlencoded")
        {
            using (var stringContent = new StringContent(content, Encoding.UTF8, contentType))
            {
                var response = await HttpClient.PostAsync(url, stringContent).ConfigureAwait(false);
                return await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            }
        }
    }
}