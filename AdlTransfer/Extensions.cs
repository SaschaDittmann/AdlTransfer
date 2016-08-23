using System;

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
    }
}
