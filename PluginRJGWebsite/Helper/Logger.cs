using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace PluginRJGWebsite.Helper
{
    public static class Logger
    {
        public enum LogLevel
        {
            Verbose,
            Debug,
            Info,
            Error,
            Off
        }

        private static string _logPrefix = "";
        private static string _path = @"plugin-rjg-website-log.txt";
        private static LogLevel _level = LogLevel.Info;
        private static ReaderWriterLockSlim _readWriteLock = new ReaderWriterLockSlim();
        private static Queue<string> _logBuffer = new Queue<string>();

        /// <summary>
        /// Writes a log message with time stamp to a file
        /// </summary>
        /// <param name="message"></param>
        private static void Log(string message)
        {
            // Set Status to Locked
            _readWriteLock.EnterWriteLock();
            try
            {
                // ensure log directory exists
                Directory.CreateDirectory("logs");

                // Append text to the file
                var filePath = $"logs/{_logPrefix}{_path}";
                using (StreamWriter sw = File.AppendText(filePath))
                {
                    sw.WriteLine($"{DateTime.Now} {message}");
                    sw.Close();
                }
            }
            catch
            {
            }
            finally
            {
                // Release lock
                _readWriteLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Deletes log file if it is older than 7 days
        /// </summary>
        public static void Clean()
        {
            if (File.Exists(_path))
            {
                if ((File.GetCreationTime(_path) - DateTime.Now).TotalDays > 7)
                {
                    File.Delete(_path);
                }
            }
        }

        /// <summary>
        /// Writes out the message buffer
        /// </summary>
        public static void WriteBuffer()
        {
            try
            {
                while (_logBuffer.Count > 0)
                {
                    Log(_logBuffer.Dequeue());
                }
            }
            catch (Exception e)
            {
                Log(e.Message);
                throw;
            }
        }

        /// <summary>
        /// Logging method for Verbose messages
        /// </summary>
        /// <param name="message"></param>
        /// <param name="buffer"></param>
        public static void Verbose(string message, bool buffer = false)
        {
            if (_level > LogLevel.Verbose)
            {
                return;
            }

            if (buffer)
            {
                _logBuffer.Enqueue(message);
                return;
            }

            Log(message);
        }

        /// <summary>
        /// Logging method for Debug messages
        /// </summary>
        /// <param name="message"></param>
        /// <param name="buffer"></param>
        public static void Debug(string message, bool buffer = false)
        {
            if (_level > LogLevel.Debug)
            {
                return;
            }

            if (buffer)
            {
                _logBuffer.Enqueue(message);
                return;
            }

            Log(message);
        }

        /// <summary>
        /// Logging method for Info messages
        /// </summary>
        /// <param name="message"></param>
        /// <param name="buffer"></param>
        public static void Info(string message, bool buffer = false)
        {
            if (_level > LogLevel.Info)
            {
                return;
            }

            if (buffer)
            {
                _logBuffer.Enqueue(message);
                return;
            }

            Log(message);
        }

        /// <summary>
        /// Logging method for Error messages
        /// </summary>
        /// <param name="message"></param>
        /// <param name="buffer"></param>
        public static void Error(string message, bool buffer = false)
        {
            if (_level > LogLevel.Error)
            {
                return;
            }

            if (buffer)
            {
                _logBuffer.Enqueue(message);
                return;
            }

            Log(message);
        }

        /// <summary>
        /// Sets the log level 
        /// </summary>
        /// <param name="level"></param>
        public static void SetLogLevel(LogLevel level)
        {
            _level = level;
        }

        /// <summary>
        /// Sets a 
        /// </summary>
        /// <param name="logPrefix"></param>
        public static void SetLogPrefix(string logPrefix)
        {
            _logPrefix = logPrefix;
        }
    }
}