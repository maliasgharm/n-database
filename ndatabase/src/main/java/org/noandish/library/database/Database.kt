package org.noandish.library.ndatabase

import android.annotation.SuppressLint
import android.content.ContentValues
import android.content.Context
import android.database.Cursor
import android.database.sqlite.SQLiteConstraintException
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteException
import android.database.sqlite.SQLiteOpenHelper
import android.os.Handler
import android.os.Looper
import android.util.Log
import org.json.JSONArray
import org.json.JSONObject
import java.sql.SQLException


/**
 * Created by AliasgharMirzazade on 10/10/2018 AD.
 */
class Database(context: Context, val table: Table) : SQLiteOpenHelper(context, DATABASE_NAME, null, DATABASE_VERSION) {
    init {

    }

    private var handler = Handler(Looper.getMainLooper())
    private var filterItem: HashMap<String, Any>? = null

    fun addFilterItem(key: String, value: Any) {
        if (filterItem == null)
            filterItem = HashMap()

        filterItem!![key] = value
    }

    private val sqlDeleteEntries = "DROP TABLE IF EXISTS " + table.table_name

    override fun onCreate(db: SQLiteDatabase) {
        db.execSQL(getQuerySalCreateEntries(table))
    }

    override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {
        // This database is only a cache for online data, so its upgrade policy is
        // to simply to discard the data and start over
        db.execSQL(sqlDeleteEntries)
        onCreate(db)
    }

    override fun onDowngrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {
        onUpgrade(db, oldVersion, newVersion)
    }

    @Throws(SQLiteConstraintException::class)
    fun insert(tableItem: HashMap<String, Any>, listener: (id: Long) -> Unit) {
        Thread {
            // Gets the data repository in write mode
            if (isTableExists()) {
                val db = writableDatabase
                if (tableItem.containsKey(KEY_ID)) {
//                    throw Exception("Can't use Database.KEY_ID or id string for key")
                    Log.e(TAG, "Can't use Database.KEY_ID or id string for key")
                }
                val values = ContentValues()
                for (item in tableItem) {
                    when {
                        item.value is String -> values.put(item.key, item.value as String)
                        item.value is Int -> values.put(item.key, item.value as Int)
                        item.value is Short -> values.put(item.key, item.value as Short)
                        item.value is Long -> values.put(item.key, item.value as Long)
                        item.value is Float -> values.put(item.key, item.value as Float)
                        item.value is Double -> values.put(item.key, item.value as Double)
                        item.value is Byte -> values.put(item.key, item.value as Byte)
                        item.value is Boolean -> values.put(item.key, item.value as Boolean)
                        item.value is ByteArray -> values.put(item.key, item.value as ByteArray)
                        item.value is JSONObject -> values.put(item.key, (item.value as JSONObject).toString())
                        item.value is JSONArray -> values.put(item.key, (item.value as JSONArray).toString())
                    }
                }
                // Insert the new row, returning the primary key value of the new row
                try {
                    val newRowId = db.insert(table.table_name, null, values)
                    handler.post {
                        listener.invoke(newRowId)
                    }
                } catch (e: SQLException) {
                    db.execSQL(getQuerySalCreateEntries(table))
                    Handler().postDelayed({ insert(tableItem, listener) }, 500)
                    e.printStackTrace()
                } finally {
                    if (db.isOpen)
                        db.close()
                }
            } else {
                val db = writableDatabase
                db.execSQL(getQuerySalCreateEntries(table))
                if (db.isOpen)
                    db.close()
                Handler().postDelayed({ insert(tableItem, listener) }, 50)
            }
        }.start()
    }

    @Suppress("unused")
    fun insert(tableItem: HashMap<String, Any>): Long {
        // Gets the data repository in write mode
        val db = writableDatabase
        if (tableItem.containsKey(KEY_ID)) {
            throw Exception("Can't use Database.KEY_ID or id string for key")
        }
        val values = ContentValues()
        for (item in tableItem) {
            when {
                item.value is String -> values.put(item.key, item.value as String)
                item.value is Int -> values.put(item.key, item.value as Int)
                item.value is Short -> values.put(item.key, item.value as Short)
                item.value is Long -> values.put(item.key, item.value as Long)
                item.value is Float -> values.put(item.key, item.value as Float)
                item.value is Double -> values.put(item.key, item.value as Double)
                item.value is Byte -> values.put(item.key, item.value as Byte)
                item.value is Boolean -> values.put(item.key, item.value as Boolean)
                item.value is ByteArray -> values.put(item.key, item.value as ByteArray)
                item.value is JSONObject -> values.put(item.key, "${(item.value as JSONObject)}")
                item.value is JSONArray -> values.put(item.key, (item.value as JSONArray).toString())
            }
        }
        // Insert the new row, returning the primary key value of the new row
        val id = db.insert(table.table_name, null, values)
        if (db.isOpen)
            db.close()
        return id
    }

    fun insertAll(tableItems: ArrayList<HashMap<String, Any>>, listener: (response: ArrayList<InsertResponseItem>) -> Unit) {
        Thread {
            // Gets the data repository in write mode
            val db = writableDatabase
            val resultSuccessInsert = ArrayList<InsertResponseItem>()

            for (tableItem in tableItems) {
                if (tableItem.containsKey(KEY_ID)) {
//                    throw Exception("Can't use Database.KEY_ID or id string for key")
                    Log.e(TAG, "Can't use Database.KEY_ID or id string for key")
                    continue
                }
                val values = ContentValues()
                for (item in tableItem) {
                    when {
                        item.value is String -> values.put(item.key, item.value as String)
                        item.value is Int -> values.put(item.key, item.value as Int)
                        item.value is Short -> values.put(item.key, item.value as Short)
                        item.value is Long -> values.put(item.key, item.value as Long)
                        item.value is Float -> values.put(item.key, item.value as Float)
                        item.value is Double -> values.put(item.key, item.value as Double)
                        item.value is Byte -> values.put(item.key, item.value as Byte)
                        item.value is Boolean -> values.put(item.key, item.value as Boolean)
                        item.value is ByteArray -> values.put(item.key, item.value as ByteArray)
                        item.value is JSONObject -> values.put(item.key, "${(item.value as JSONObject)}")
                        item.value is JSONArray -> values.put(item.key, (item.value as JSONArray).toString())
                    }
                }
                // Insert the new row, returning the primary key value of the new row
                val newRowId = InsertResponseItem(db.insert(table.table_name, null, values), true)
                resultSuccessInsert.add(newRowId)
            }

            if (db.isOpen)
                db.close()
            handler.post {
                listener.invoke(resultSuccessInsert)
            }
        }.start()
    }

    @Suppress("unused")
    fun insertAll(tableItems: ArrayList<HashMap<String, Any>>): ArrayList<InsertResponseItem> {
        // Gets the data repository in write mode
        val db = writableDatabase
        val resulteSuccessInsert = ArrayList<InsertResponseItem>()
        for (tableItem in tableItems) {
            if (tableItem.containsKey(KEY_ID)) {
                throw Exception("Can't use Database.KEY_ID or id string for key")
            }
            val values = ContentValues()
            for (item in tableItem) {
                when {
                    item.value is String -> values.put(item.key, item.value as String)
                    item.value is Int -> values.put(item.key, item.value as Int)
                    item.value is Short -> values.put(item.key, item.value as Short)
                    item.value is Long -> values.put(item.key, item.value as Long)
                    item.value is Float -> values.put(item.key, item.value as Float)
                    item.value is Double -> values.put(item.key, item.value as Double)
                    item.value is Byte -> values.put(item.key, item.value as Byte)
                    item.value is Boolean -> values.put(item.key, item.value as Boolean)
                    item.value is ByteArray -> values.put(item.key, item.value as ByteArray)
                    item.value is JSONObject -> values.put(item.key, "${(item.value as JSONObject)}")
                    item.value is JSONArray -> values.put(item.key, (item.value as JSONArray).toString())
                }
            }
            // Insert the new row, returning the primary key value of the new row
            val newRowId = InsertResponseItem(db.insert(table.table_name, null, values), true)
            resulteSuccessInsert.add(newRowId)
        }
        if (db.isOpen)
            db.close()

        return resulteSuccessInsert

    }

    /**
     * hashMap.put(KEY_TABLE_NAME,string) and hashMap.put(KEY_ID,int)
     *@sample delete(hashMap,response)
     */
    @Throws(SQLiteConstraintException::class)
    fun delete(hashMap: HashMap<String, Any>, listener: (response: Int) -> Unit) {
        if (!hashMap.containsKey(KEY_ID)) {
            throw Exception("KEY_ID can't find ; should put KEY_ID in HashMap")
        }
        Thread {
            val selection = "$KEY_ID = ?"
            // Specify arguments in placeholder order.
            val db = writableDatabase

            val result = db.delete(table.table_name, selection, arrayOf(hashMap[KEY_ID].toString()))

            if (db.isOpen)
                db.close()
            handler.post {
                listener.invoke(result)
            }

        }.start()
    }

    @Throws(SQLiteConstraintException::class)
    fun delete(hashMap: HashMap<String, Any>): Int {
        if (!hashMap.containsKey(KEY_ID)) {
            throw Exception("KEY_ID can't find ; should put KEY_ID in HashMap")
        }
        val hashMaps = ArrayList<HashMap<String, Any>>()
        hashMaps.add(hashMap)

        val ids = arrayOfNulls<String>(hashMaps.size)
        for (item in 0 until hashMaps.size) {
            ids[item] = hashMaps[item].toString()
        }
        val selection = "$KEY_ID LIKE ?"
        // Specify arguments in placeholder order.
        val db = writableDatabase

        val result = db.delete(table.table_name, selection, ids)
        if (db.isOpen)
            db.close()
        return result
    }

    /**
     * return All id Success deleted
     */
    @Suppress("MemberVisibilityCanBePrivate")
    fun delete(hashMaps: ArrayList<HashMap<String, Any>>, listener: (response: Int) -> Unit) {
        Thread {

            var result = -3
            if (isTableExists(table.table_name)) {
                val ids = arrayOfNulls<String>(hashMaps.size)
                for (item in 0 until hashMaps.size) {
                    ids[item] = hashMaps[item].toString()
                }
                val selection = "$KEY_ID LIKE ?"
                // Specify arguments in placeholder order.
                val db = writableDatabase
                result = db.delete(table.table_name, selection, ids)
                Log.w(TAG, "close 3")
                if (db.isOpen)
                    db.close()
            }
            handler.post {
                listener.invoke(result)
            }
        }.start()

    }

    @Suppress("MemberVisibilityCanBePrivate")
    fun delete(hashMaps: ArrayList<HashMap<String, Any>>): Int {
        var result = -3
        if (isTableExists(table.table_name)) {
            val ids = arrayOfNulls<String>(hashMaps.size)
            for (item in 0 until hashMaps.size) {
                ids[item] = hashMaps[item].toString()
            }
            val selection = "$KEY_ID LIKE ?"
            // Specify arguments in placeholder order.
            val db = writableDatabase
            result = db.delete(table.table_name, selection, ids)
            if (db.isOpen)
                db.close()

        }
        return result
    }

    fun deleteAll(listener: (response: Int) -> Unit) {
        Thread {
            // Specify arguments in placeholder order.
            var result = -3
            if (isTableExists(table.table_name)) {
                val db = writableDatabase
                result = db.delete(table.table_name, null, null)
                if (db.isOpen)
                    db.close()
            }

            handler.post {
                listener.invoke(result)
            }
        }.start()
    }

    fun deleteAll(): Int {
        // Specify arguments in placeholder order.
        var result = -3
        if (isTableExists(table.table_name)) {
            val db = writableDatabase
            result = db.delete(table.table_name, null, null)
            if (db.isOpen)
                db.close()
        }
        Log.w(TAG, "delete table : ${table.table_name}, result : $result")
        return result
    }

    @SuppressLint("Recycle")
    fun read(id: Int, listener: (items: HashMap<String, Any>) -> Unit) {
        Thread {
            val item = HashMap<String, Any>()
            val db = writableDatabase
            try {
                val cursor = db.rawQuery("select * from ${table.table_name} WHERE $KEY_ID='$id' ${filter()}", null)
                if (cursor!!.moveToFirst()) {
                    while (!cursor.isAfterLast) {
                        for (columnNames in cursor.columnNames) {
                            item[columnNames] = cursor.getColumnIndex(columnNames)
                        }
                        cursor.moveToNext()
                    }
                }

            } catch (e: SQLiteException) {
                // if table not yet present, create it
                db.execSQL(getQuerySalCreateEntries(table))
            } finally {
                if (db.isOpen)
                    db.close()
            }
            handler.post {
                listener.invoke(item)
            }
        }.start()
    }

    private fun filter(withAndFirst: Boolean = true): String {
        var mWithAndFirst = withAndFirst
        var filter = ""
        val keys = filterItem?.keys ?: return ""

        for (key in keys) {


//            when {
//                filterItem!![key] is String -> newValue.put(item.key, item.value as String)
//                filterItem!![key] is Int -> newValue.put(item.key, item.value as Int)
//                filterItem!![key] is Short -> newValue.put(item.key, item.value as Short)
//                filterItem!![key] is Long -> newValue.put(item.key, item.value as Long)
//                filterItem!![key] is Float -> newValue.put(item.key, item.value as Float)
//                filterItem!![key] is Double -> newValue.put(item.key, item.value as Double)
//                filterItem!![key] is Byte -> newValue.put(item.key, item.value as Byte)
//                filterItem!![key] is Boolean -> newValue.put(item.key, item.value as Boolean)
//                filterItem!![key] is ByteArray -> newValue.put(item.key, item.value as ByteArray)
//                filterItem!![key] is JSONObject -> newValue.put(item.key, "${(item.value as JSONObject)}")
//                filterItem!![key] is JSONArray -> newValue.put(item.key, (item.value as JSONArray).toString())
//            }


            filter += (if (mWithAndFirst) {
                mWithAndFirst = true; " or "
            } else "") + "  $key='${filterItem!![key]}' "


        }
        return filter
    }

    @Suppress("unused")
    @SuppressLint("Recycle")
    fun read(id: Int): HashMap<String, Any> {
        val item = HashMap<String, Any>()
        val db = writableDatabase
        try {
            val cursor = db.rawQuery("select * from ${table.table_name} WHERE $KEY_ID='$id' ${filter()}  ", null)
            if (cursor!!.moveToFirst()) {
                while (!cursor.isAfterLast) {
                    for (columnNames in cursor.columnNames) {
                        item[columnNames] = cursor.getColumnIndex(columnNames)
                    }
                    cursor.moveToNext()
                }
            }
        } catch (e: SQLiteException) {
            // if table not yet present, create it
            db.execSQL(getQuerySalCreateEntries(table))
        }
        if (db.isOpen)
            db.close()
        return item
    }

    fun isTableExists(tableName: String = table.table_name): Boolean {
        var mDatabase = readableDatabase
        if (!mDatabase.isReadOnly) {
            if (mDatabase.isOpen)
                mDatabase.close()
            mDatabase = readableDatabase
        }

        val cursor =
                mDatabase.rawQuery("select DISTINCT tbl_name from sqlite_master where tbl_name = '$tableName'", null)
        if (cursor != null) {
            if (cursor.count > 0) {
                if (!cursor.isClosed)
                    cursor.close()
                return true
            }
            if (!cursor.isClosed)
                cursor.close()

        }
        return false
    }

    @SuppressLint("Recycle")
    fun readAll(listener: (items: ArrayList<HashMap<String, Any>>) -> Unit) {
        val items = ArrayList<HashMap<String, Any>>()
        val db = writableDatabase
        try {
            val mFilter = filter(false)
            val filter = if (mFilter == "") "" else " where $mFilter"
            val cursor =
                    db.rawQuery("select * from ${table.table_name} $filter", null)
            if (cursor!!.moveToFirst()) {
                while (!cursor.isAfterLast) {
                    val item = HashMap<String, Any>()
                    for (columnName in cursor.columnNames) {
                        row@ for (row in table.rows) {
                            if (row.name_row == columnName || columnName == KEY_ID) {
                                item[columnName] = changeCursorToHashMap(if (columnName == KEY_ID) null else row, cursor)
                                break@row
//                            item[columnName] = cursor.getString(cursor.getColumnIndex(columnName))
                            }
                        }
                    }
                    items.add(item)
                    cursor.moveToNext()
                }
            }
        } catch (e: SQLiteException) {
            db.execSQL(getQuerySalCreateEntries(table))
//            response.read(ArrayList())
        } finally {
            if (db.isOpen)
                db.close()
        }
        handler.post {
            listener.invoke(items)
        }
    }

    private fun changeCursorToHashMap(row: Row?, cursor: Cursor): Any {

        return when (row?.type_row) {
            null -> cursor.getInt(cursor.getColumnIndex(KEY_ID))
            Row.TYPE_INTEGER -> cursor.getInt(cursor.getColumnIndex(row.name_row))
            else -> cursor.getString(cursor.getColumnIndex(row.name_row))
        }

    }

    @SuppressLint("Recycle")
    fun readAll(): ArrayList<HashMap<String, Any>> {
        val items = ArrayList<HashMap<String, Any>>()
        val db = writableDatabase
        try {

            val mFilter = filter(false)
            val filter = if (mFilter == "") "" else " where $mFilter"
            val cursor = db.rawQuery("select * from ${table.table_name} $filter", null)
            if (cursor!!.moveToFirst()) {
                while (!cursor.isAfterLast) {
                    val item = HashMap<String, Any>()
                    for (columnName in cursor.columnNames) {
                        item[columnName] = cursor.getString(cursor.getColumnIndex(columnName))
                    }
                    items.add(item)
                    cursor.moveToNext()
                }
            }
        } catch (e: SQLiteException) {
            db.execSQL(getQuerySalCreateEntries(table))
//            response.read(ArrayList())
        } finally {
            if (db.isOpen)
                db.close()
        }
        return items
    }

    /**
     *  [tableItem] should with id  for update
     */
    @Suppress("unused")
    fun update(tableItem: HashMap<String, Any>, listener: (response: Boolean) -> Unit) {
        Thread {
            val db = writableDatabase
            if (!tableItem.containsKey(KEY_ID)) {
                throw Exception("tableItem can't found id")
            }
            val newValue = ContentValues()
            for (item in tableItem) {
                when {
                    item.value is String -> newValue.put(item.key, item.value as String)
                    item.value is Int -> newValue.put(item.key, item.value as Int)
                    item.value is Short -> newValue.put(item.key, item.value as Short)
                    item.value is Long -> newValue.put(item.key, item.value as Long)
                    item.value is Float -> newValue.put(item.key, item.value as Float)
                    item.value is Double -> newValue.put(item.key, item.value as Double)
                    item.value is Byte -> newValue.put(item.key, item.value as Byte)
                    item.value is Boolean -> newValue.put(item.key, item.value as Boolean)
                    item.value is ByteArray -> newValue.put(item.key, item.value as ByteArray)
                    item.value is JSONObject -> newValue.put(item.key, "${(item.value as JSONObject)}")
                    item.value is JSONArray -> newValue.put(item.key, (item.value as JSONArray).toString())
                }
            }

            val success =
                    db.update(table.table_name, newValue, KEY_ID + "=" + tableItem[KEY_ID] + " ${filter()}", null) > 0

            if (db.isOpen)
                db.close()
            handler.post {
                listener.invoke(success)
            }
        }.start()
    }

    @Suppress("unused")
    fun update(tableItem: HashMap<String, Any>): Boolean {
        val db = writableDatabase
        if (!tableItem.containsKey(KEY_ID))
            throw Exception("tableItem can't found id")
        val newValue = ContentValues()
        for (item in tableItem) {
            when {
                item.value is String -> newValue.put(item.key, item.value as String)
                item.value is Int -> newValue.put(item.key, item.value as Int)
                item.value is Short -> newValue.put(item.key, item.value as Short)
                item.value is Long -> newValue.put(item.key, item.value as Long)
                item.value is Float -> newValue.put(item.key, item.value as Float)
                item.value is Double -> newValue.put(item.key, item.value as Double)
                item.value is Byte -> newValue.put(item.key, item.value as Byte)
                item.value is Boolean -> newValue.put(item.key, item.value as Boolean)
                item.value is ByteArray -> newValue.put(item.key, item.value as ByteArray)
                item.value is JSONObject -> newValue.put(item.key, "${(item.value as JSONObject)}")
                item.value is JSONArray -> newValue.put(item.key, (item.value as JSONArray).toString())
            }
        }

        val response = db.update(table.table_name, newValue, KEY_ID + "=" + tableItem[KEY_ID] + " ${filter()} ", null) > 0
        if (db.isOpen)
            db.close()
        return response
    }

    /**
     * [Table.rows] is  HashMap< nameRow , TypeRow >
     */
    private fun getQuerySalCreateEntries(table: Table): String {

        var sqlCreateEntries = "CREATE TABLE " + table.table_name + " ( " +
                KEY_ID + "  integer primary key autoincrement, "
        for (items in table.rows) {
            when (items.type_row) {
                Row.TYPE_STRING -> {
                    sqlCreateEntries += " ${items.name_row} text,"
                }
                Row.TYPE_INTEGER -> {
                    sqlCreateEntries += " ${items.name_row} integer,"
                }
            }
        }
        if (table.rows.size > 0 && sqlCreateEntries.substring(
                        sqlCreateEntries.length - 1,
                        sqlCreateEntries.length) == ","
        )
            sqlCreateEntries = sqlCreateEntries.substring(0, sqlCreateEntries.length - 1)

        val tableParams = "$sqlCreateEntries );"
        Log.w(TAG, "tablePrams : $tableParams")
        return tableParams
    }

    fun create() {
        val db = writableDatabase
        db.execSQL(getQuerySalCreateEntries(table))
        if (db.isOpen)
            db.close()
    }

    companion object {
        private const val DATABASE_VERSION = 4
        private const val DATABASE_NAME = "FeedReader3.db"
        private const val TAG = "Database"
        const val KEY_ID = "id"
    }

}