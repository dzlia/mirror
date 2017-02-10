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
#include "FileDB.hpp"
#include <afc/StringRef.hpp>
#include <afc/logger.hpp>

using afc::operator"" _s;
using afc::logger::logDebug;

mirror::FileDB::FileDB(const char * const dbPathInUtf8)
{
	constexpr auto createFileTableQuery = u8"create table if not exists files "
			"(path primary key asc, size integer not null, last_modified integer not null, md5 blob not null)"_s;
	constexpr auto addFileQuery = u8"insert or replace into files (path, size, last_modified, md5) values (?, ?, ?, ?)"_s;
	constexpr auto getFileQuery = u8"select * from files where path = ?"_s;

	int result;

	logDebug("Opening connection to the DB "_s, dbPathInUtf8);
	result = sqlite3_open(afc::convertToUtf8(dbPathInUtf8, afc::systemCharset().c_str()).c_str(), &m_conn);
	logDebug("Result code: "_s, result);

	if (result != SQLITE_OK) {
		goto error_openConn;
	}

	logDebug("Creating the file table (if missing): "_s, createFileTableQuery);
	result = sqlite3_exec(m_conn, createFileTableQuery.value(), nullptr, nullptr, nullptr);
	logDebug("Result code: "_s, result);

	if (result != SQLITE_OK) {
		goto error_initFileTable;
	}

	logDebug("Preparing statement to add a file: "_s, addFileQuery);
	result = sqlite3_prepare_v2(m_conn, addFileQuery.value(), addFileQuery.size(), &m_addFileStmt, nullptr);
	logDebug("Result code: "_s, result);

	if (result != SQLITE_OK) {
		goto error_addFileStmt;
	}

	logDebug("Preparing statement to get a file: "_s, getFileQuery);
	result = sqlite3_prepare_v2(m_conn, getFileQuery.value(), getFileQuery.size(), &m_getFileStmt, nullptr);
	logDebug("Result code: "_s, result);

	if (result != SQLITE_OK) {
		goto error_getFileStmt;
	}

	return;

error_getFileStmt:
	sqlite3_finalize(m_addFileStmt);
error_addFileStmt:
error_initFileTable:
	sqlite3_close(m_conn);
error_openConn:
	// TODO handle error.
	throw sqlite3_errstr(result);
}

void mirror::FileDB::addFile(const char * const fileName, const std::size_t fileNameSize, const FileRecord &data)
{
	assert(m_conn != nullptr);

	int result;

	logDebug("Binding statement param 1...");
	result = sqlite3_bind_text(m_addFileStmt, 1, fileName, fileNameSize, SQLITE_STATIC);
	if (result != SQLITE_OK) {
		goto handle_error;
	}

	logDebug("Binding statement param 2...");
	result = sqlite3_bind_int64(m_addFileStmt, 2, static_cast<sqlite_int64>(data.fileSize));
	if (result != SQLITE_OK) {
		goto handle_error;
	}

	logDebug("Binding statement param 3...");
	result = sqlite3_bind_int64(m_addFileStmt, 3, static_cast<sqlite_int64>(data.lastModifiedTS.millis() / 1000));
	if (result != SQLITE_OK) {
		goto handle_error;
	}

	logDebug("Binding statement param 4...");
	result = sqlite3_bind_blob(m_addFileStmt, 4, data.md5Digest, MD5_DIGEST_LENGTH, SQLITE_STATIC);
	if (result != SQLITE_OK) {
		goto handle_error;
	}

	logDebug("Executing statement...");
	result = sqlite3_step(m_addFileStmt);
	if (result != SQLITE_DONE) {
		goto handle_error;
	}

	logDebug("Reseting statement...");
	result = sqlite3_reset(m_addFileStmt);
	if (result != SQLITE_OK) {
		goto handle_reset_error;
	}

	return;

handle_error:
	// Attempting to reset the statement without overwriting the error code.
	logDebug("Reseting statement...");
	// TODO handle sqlite3_reset error code.
	sqlite3_reset(m_addFileStmt);
handle_reset_error:
	throw sqlite3_errstr(result);
}
