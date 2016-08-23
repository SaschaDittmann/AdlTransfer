using System;
using System.Security;

namespace AdlTransfer
{
    public static class Extensions
    {
        public static string ToSizeString(this long size)
        {
            string[] sizes = { "B", "KB", "MB", "GB", "TB", "PB", "EB" };
            if (size == 0)
                return $"0 {sizes[0]}";
            var bytes = Math.Abs(size);
            var place = Convert.ToInt32(Math.Floor(Math.Log(bytes, 1024)));
            var num = Math.Round(bytes / Math.Pow(1024, place), 1);
            return $"{(Math.Sign(size) * num):0.##} {sizes[place]}";
        }

        public static SecureString ToSecureString(this string password)
        {
            if (password == null)
                return new SecureString();

            var securePassword = new SecureString();
            foreach (var c in password)
                securePassword.AppendChar(c);
            securePassword.MakeReadOnly();
            return securePassword;
        }
    }
}
