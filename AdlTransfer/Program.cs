using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Security;
using Microsoft.Azure.Common.Authentication;
using Microsoft.Azure.Common.Authentication.Factories;
using Microsoft.Azure.Common.Authentication.Models;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Azure.Management.DataLake.StoreUploader;
using Microsoft.Rest;
using NDesk.Options;

namespace AdlTransfer
{
    class Program
    {
        private static string _sourcePath;
        private static string _targetPath;
        private static string _accountName;
        private static string _userName;
        private static SecureString _password;
        private static string _tenantId;
        private static bool _isServicePrincipal;
        private static int _perFileThreadCount = 10;
        private static int _concurrentFileCount = 5;
        private static bool _isOverwrite;
        private static bool _isResume;
        private static bool _isBinary;
        private static bool _isRecursive;
        private static bool _isDownload;
        private static long _maxSegmentLength = 268435456;
        private static string _localMetadataLocation = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        private static bool _verbose;

        private static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("------------------------------------------------------------------------------");
                Console.WriteLine($"AdlTransfer {Assembly.GetEntryAssembly().GetName().Version} Copyright (c) 2016 Sascha Dittmann. All Rights Reserved.");
                Console.WriteLine("------------------------------------------------------------------------------");

                if (ParseArguments(args))
                    return;

                Console.WriteLine();
                Console.WriteLine($"Source: {_sourcePath}");
                Console.WriteLine($"Target: {_targetPath}");
                Console.WriteLine($"Account Name: {_accountName}");
                if (_verbose)
                {
                    Console.WriteLine();
                    Console.WriteLine($"Per File Thread Count: {_perFileThreadCount}");
                    Console.WriteLine($"Concurrent File Count: {_concurrentFileCount}");
                    Console.WriteLine($"Segment Length: {_maxSegmentLength.ToSizeString()}");
                    Console.WriteLine();
                    Console.WriteLine($"Overwrite: {_isOverwrite}");
                    Console.WriteLine($"Binary: {_isBinary}");
                    Console.WriteLine($"Recursive: {_isRecursive}");
                }
                Console.WriteLine();

                var credentials = Authenticate();
                var client = new DataLakeStoreFileSystemManagementClient(credentials);
                var frontEndAdapter = new DataLakeStoreFrontEndAdapter(_accountName, client);

                var uploadParameters = new UploadParameters(
                    _sourcePath,
                    _targetPath,
                    _accountName,
                    _perFileThreadCount,
                    _concurrentFileCount,
                    _isOverwrite,
                    _isResume,
                    _isBinary,
                    _isRecursive,
                    _isDownload,
                    _maxSegmentLength,
                    _localMetadataLocation);

                var progressTracker = new Progress<UploadProgress>();
                progressTracker.ProgressChanged += UploadProgressChanged;

                var folderProgressTracker = new Progress<UploadFolderProgress>();
                folderProgressTracker.ProgressChanged += UploadFolderProgressChanged;

                var uploader = new DataLakeStoreUploader(uploadParameters, frontEndAdapter, progressTracker, folderProgressTracker);

                Console.WriteLine($"{(_isResume ? "Resuming" : "Starting")} {(_isDownload ? "Download" : "Upload")}...");
                uploader.Execute();
                Console.WriteLine($"{(_isDownload ? "Download" : "Upload")} completed.");
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Error.WriteLine(e.Message);
                Console.ResetColor();
                Environment.ExitCode = -1;
            }
        }

        private static bool ParseArguments(IEnumerable<string> args)
        {
            try
            {
                var showHelp = false;
                var showSamples = false;

                var optionSet = new OptionSet
                {
                    {"u|user=", "The {name} of Azure Active Directory user\nor the client id of the Service Principal.", v => _userName = v},
                    {"p|password=", "The {password} of Azure Active Directory user\nor the authentication key of the Service Principal.", v => _password = v.ToSecureString()},
                    {"t|tenant=", "The {id} of Azure Active Directory tenant.", v => _tenantId = v},
                    {"i|spi|serviceprincipal", "Use an Azure Active Directory Service Principal to authenticate.\nFor more details how to create a Service Principal see https://azure.microsoft.com/en-us/documentation/articles/resource-group-authenticate-service-principal/", v => _isServicePrincipal = v != null},

                    {"f|filethreads=", "The maximum {count} of threads used to upload each file.", (int v) => _perFileThreadCount = v},
                    {"c|concurrentfiles=", "The maximum {number} of concurrent file uploads.", (int v) => _concurrentFileCount = v},
                    {"s|segment=", "The maximum {length} of each segement (in bytes).\nThe default is 256mb, which gives you optimal performance.", (long v) => _maxSegmentLength = v},

                    {"b|binary", "The input file will be treated as a binary.\nOtherwise the file will be treated as delimited input.", v => _isBinary = v != null},
                    {"o|overwrite", "Overwrite the target stream, if it already exists.", v => _isOverwrite = v != null},
                    {"r|recursive", "Recursively upload the source folder and its subfolders.\nThis is only valid for folder uploads and will be ignored for file uploads.", v => _isRecursive = v != null},
                    {"resume", "Resume a previously interrupted upload.", v => _isResume = v != null},

                    {"d|download", "Download the file(s) instead of uploading.", v => _isDownload = v != null},
                    {"m|metadata=", "The directory {path} where to store the local upload metadata file while the upload is in progress.", v => _localMetadataLocation = v},

                    {"v|verbose", "Provide detailed status messages.", v => _verbose = v != null},

                    {"h|?|help", "Show this help text.", v => showHelp = v != null},
                    {"samples", "Show Command line samples.", v => showSamples = v != null},
                };
                var extras = optionSet.Parse(args);

                if (showSamples)
                {
                    ShowSamples();
                    return true;
                }

                if (extras.Count < 3 || showHelp)
                {
                    ShowHelp(optionSet);
                    return true;
                }

                _sourcePath = extras[0];
                _targetPath = extras[1];
                _accountName = extras[2];

                if (!_isDownload && !Directory.Exists(_sourcePath) && !File.Exists(_sourcePath))
                {
                    Console.Error.WriteLine("The source file or folder does not exist.");
                    Environment.ExitCode = -1;
                    return true;
                }

                //if (_isDownload && !Directory.Exists(_targetPath) && !File.Exists(_targetPath))
                //{
                //    Console.Error.WriteLine("The target file or folder does not exist.");
                //    Environment.ExitCode = -1;
                //    return true;
                //}

                if (_isServicePrincipal && (string.IsNullOrEmpty(_userName) || _password == null || _password.Length == 0 || string.IsNullOrEmpty(_tenantId)))
                {
                    Console.Error.WriteLine("Please specify the client id, tenant id and authentication key using the -u, -t and -p options.");
                    Environment.ExitCode = -1;
                    return true;
                }

                return false;
            }
            catch (OptionException e)
            {
                Console.Error.WriteLine($"AdlTransfer: {e.Message}");
                Console.Error.WriteLine("Try `AdlTransfer --help' for more information.");
                Environment.ExitCode = -1;
                return true;
            }
        }

        private static void ShowHelp(OptionSet optionSet)
        {
            Console.WriteLine("AdlTransfer is designed for high-performance uploading and downloading");
            Console.WriteLine("data to and from Microsoft Azure Data Lake Store.");
            Console.WriteLine();
            Console.WriteLine("# Command Line Usage:");
            Console.WriteLine("  AdlTransfer {Source} {Target} {AccountName} [OPTIONS]");
            Console.WriteLine();
            Console.WriteLine("# Options:");
            optionSet.WriteOptionDescriptions(Console.Out);
        }

        private static void ShowSamples()
        {
            Console.WriteLine();
            Console.WriteLine("##");
            Console.WriteLine("## Samples ##");
            Console.WriteLine("##");
            Console.WriteLine();
            Console.WriteLine("# Uploading all files from a local folder to Azure Data Lake Store");
            Console.WriteLine("#   using the logged in user or asking for user credentials, for example,");
            Console.WriteLine("#   upload 'C:\\MyLocalPath\\' to '/MyRemotePath'");
            Console.WriteLine("  AdlTransfer C:\\MyLocalPath\\ /MyRemotePath MyAdlAccountName");
            Console.WriteLine();
            Console.WriteLine("# Uploading all files from a local folder to Azure Data Lake Store");
            Console.WriteLine("#   using a user name and password, for example, ");
            Console.WriteLine("#   upload 'C:\\MyLocalPath\\' to '/MyRemotePath'");
            Console.WriteLine("  AdlTransfer C:\\MyLocalPath\\ /MyRemotePath MyAdlAccountName -u MyUserName -p MyPassword");
            Console.WriteLine();
            Console.WriteLine("# Uploading a single file from a local folder to Azure Data Lake Store");
            Console.WriteLine("#   using the logged in user or asking for user credentials, for example,");
            Console.WriteLine("#   upload 'C:\\MyLocalPath\\MyFile.txt' to '/MyRemotePath/MyFile.txt'");
            Console.WriteLine("  AdlTransfer C:\\MyLocalPath\\MyFile.txt /MyRemotePath/MyFile.txt MyAdlAccountName");
            Console.WriteLine();
            Console.WriteLine("# Downloading all files from a path within Azure Data Lake Store to a local folder");
            Console.WriteLine("#   using the logged in user or asking for user credentials, for example, ");
            Console.WriteLine("#   download '/MyRemotePath' to 'C:\\MyLocalPath\\'");
            Console.WriteLine("  AdlTransfer /MyRemotePath C:\\MyLocalPath MyAdlAccountName -d");
            Console.WriteLine();
            Console.WriteLine("# Downloading all files from a path within Azure Data Lake Store to a local folder");
            Console.WriteLine("#   using an Azure Active Directory Service Principal, for example, ");
            Console.WriteLine("#   download '/MyRemotePath' to 'C:\\MyLocalPath\\'");
            Console.WriteLine("  AdlTransfer /MyRemotePath C:\\MyLocalPath MyAdlAccountName -u {ClientId} -p {AuthenticationKey} -spi -d");
        }

        private static void UploadProgressChanged(object sender, UploadProgress e)
        {

            if (e.UploadedByteCount == 0)
                return;

            var percent = (double)e.UploadedByteCount / e.TotalFileLength * 100.0;

            Console.WriteLine($"{percent:0.##}%, {e.UploadedByteCount}/{e.TotalFileLength} bytes, {e.TotalSegmentCount} segment(s)");
        }

        private static void UploadFolderProgressChanged(object sender, UploadFolderProgress e)
        {
            if (e.UploadedByteCount == 0)
                return;

            var percent = (double)e.UploadedByteCount / e.TotalFileLength * 100.0;

            Console.WriteLine($"{percent:0.##}%, {e.UploadedFileCount}/{e.TotalFileCount} files, {e.UploadedByteCount}/{e.TotalFileLength} bytes");
        }

        private static TokenCredentials Authenticate()
        {
            var authFactory = new AuthenticationFactory();
            var account = new AzureAccount
            {
                Type = _isServicePrincipal
                    ? AzureAccount.AccountType.ServicePrincipal
                    : AzureAccount.AccountType.User
            };

            if (_userName != null && (_password != null || _isServicePrincipal))
                account.Id = _userName;

            var env = AzureEnvironment.PublicEnvironments[EnvironmentName.AzureCloud];

            var tenant = string.IsNullOrEmpty(_tenantId)
                ? AuthenticationFactory.CommonAdTenant
                : _tenantId;

            ShowDialog showDialog;
            if (_isServicePrincipal)
                showDialog = ShowDialog.Never;
            else if (_userName != null && _password == null)
                showDialog = ShowDialog.Always;
            else
                showDialog = ShowDialog.Auto;

            var authResult = authFactory.Authenticate(
                account, env, tenant, _password, showDialog);

            return new TokenCredentials(authResult.AccessToken);
        }
    }
}
