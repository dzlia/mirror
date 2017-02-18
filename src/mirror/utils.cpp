/* mirror - a tool to make mirrors of files or directories and to check consistency of the existing mirrors.
Copyright (C) 2017 Dźmitry Laŭčuk

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */
#include "utils.hpp"
#include <cstring>
#include <openssl/md5.h>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/types.h>

using afc::operator"" _s;
using afc::logger::logDebug;

[[noreturn]]
void mirror::_helper::handleOpenFileError(int errorCode)
{
	const char *msg;
	switch (errno) {
	case EACCES:
		msg = "Search permission is denied on a component of the path prefix, or the file exists "
				"and the permissions specified by mode are denied, or the file does not exist "
				"and write permission is denied for the parent directory of the file to be created.";
		break;
	case EINTR:
		msg = "A signal was caught during fopen().";
		break;
	case EISDIR:
		msg = "The named file is a directory and mode requires write access.";
		break;
	case ELOOP:
		msg = "A loop exists in symbolic links encountered during resolution of the path argument OR "
				"more than {SYMLOOP_MAX} symbolic links were encountered during resolution of the path argument.";
		break;
	case EMFILE:
		msg = "{OPEN_MAX} file descriptors are currently open in the calling process OR "
				"{FOPEN_MAX} streams are currently open in the calling process OR "
				"{STREAM_MAX} streams are currently open in the calling process.";
		break;
	case ENAMETOOLONG:
		msg = "The length of the filename argument exceeds {PATH_MAX} or a pathname component is "
				"longer than {NAME_MAX} OR pathname resolution of a symbolic link produced an intermediate "
				"result whose length exceeds {PATH_MAX}.";
		break;
	case ENFILE:
		msg = "The maximum allowable number of files is currently open in the system.";
		break;
	case ENOENT:
		msg = "A component of filename does not name an existing file or filename is an empty string.";
		break;
	case ENOSPC:
		msg = "The directory or file system that would contain the new file cannot be expanded, the file "
				"does not exist, and the file was to be created.";
		break;
	case ENOTDIR:
		msg = "A component of the path prefix is not a directory.";
		break;
	case ENXIO:
		msg = "The named file is a character special or block special file, and the device associated with "
				"this special file does not exist.";
		break;
	case EOVERFLOW:
		msg = "The named file is a regular file and the size of the file cannot be represented correctly in "
				"an object of type off_t.";
		break;
	case EROFS:
		msg = "The named file resides on a read-only file system and mode requires write access.";
		break;
	case EINVAL:
		msg = "The value of the mode argument is not valid.";
		break;
	case ENOMEM:
		msg = "Insufficient storage space is available.";
		break;
	case ETXTBSY:
		msg = "The file is a pure procedure (shared text) file that is being executed and mode requires "
				"write access.";
		break;
	default:
		// TODO add error code to the message.
		msg = "Unexpected error has occurred.";
		break;
	}

	throw std::runtime_error(msg);
}

[[noreturn]]
void mirror::_helper::handleReadFileError(int errorCode)
{
	const char *msg;
	switch (errorCode) {
	case EAGAIN:
		msg = "The O_NONBLOCK flag is set for the file descriptor underlying stream and the thread would be "
				"delayed in the fgetc() operation.";
		break;
	case EBADF:
		msg = "The file descriptor underlying stream is not a valid file descriptor open for reading.";
		break;
	case EINTR:
		msg = "The read operation was terminated due to the receipt of a signal, and no data was transferred.";
		break;
	case EIO:
		msg = "A physical I/O error has occurred, or the process is in a background process group attempting "
				"to read from its controlling terminal, and either the process is ignoring or blocking "
				"the SIGTTIN signal or the process group is orphaned. This error may also be generated "
				"for implementation-defined reasons.";
		break;
	case EOVERFLOW:
		msg = "The file is a regular file and an attempt was made to read at or beyond the offset maximum "
				"associated with the corresponding stream.";
		break;
	case ENOMEM:
		msg = "Insufficient storage space is available.";
		break;
	case ENXIO:
		msg = "A request was made of a nonexistent device, or the request was outside the capabilities "
				"of the device.";
		break;
	default:
		// TODO add error code to the message.
		msg = "Unexpected error has occurred.";
		break;
	}

	throw std::runtime_error(msg);
}

void mirror::createDB(const char * const rootDir, mirror::FileDB &db)
{
	auto addFileOp = [&db] (const char * const rootDir, const char * const relDir, const char * const fileName)
	{
		logDebug("Adding the file '"_s, fileName, "' to the DB."_s);

		// TODO avoid unnecessary memory allocations.
		const std::string absolutePath = (std::string(rootDir) + '/') + fileName;
		struct stat fileStat;
		const int result = lstat(absolutePath.c_str(), &fileStat);
		if (result != 0) {
			switch (errno) {
			case EACCES:
				// TODO make the behaviour configurable.
				logDebug("No access to '"_s, absolutePath.c_str(), '\'');
				return;
			default:
				// TODO handle error
				logDebug(errno);
				throw errno;
			}
		}
		// TODO check if this file is still a regular file.
		// TODO calculate MD5
		mirror::FileRecord fileRecord;
		fileRecord.fileSize = fileStat.st_size;
		fileRecord.lastModifiedTS.setMillis(static_cast<afc::Timestamp::time_type>(fileStat.st_mtime) * 1000);

		MD5_CTX md5Ctx;
		auto calcMD5 = [&md5Ctx] (const char buf[], const std::size_t n) noexcept { MD5_Update(&md5Ctx, buf, n); };

		MD5_Init(&md5Ctx);
		mirror::_helper::processFile(absolutePath.c_str(), calcMD5);
		MD5_Final(fileRecord.md5Digest, &md5Ctx);

		// TODO avoid std::strlen() to optimise performance.
		db.addFile(fileName, std::strlen(fileName), relDir, std::strlen(relDir), fileRecord);
	};

	mirror::_helper::scanFiles(rootDir, "", addFileOp);
}