<?php
/**
 * Created by thepizzy.net
 * User: @neotsn
 * Date: 5/18/2014
 * Time: 3:35 PM
 */
namespace Mantle\Data;

use \PDO;
use \PDOException;

/**
 * Set some defined statements
 */

// Modify Statements
define('SQL_CREATE_TABLE_GENERIC', 'CREATE TABLE IF NOT EXISTS %tn (%ct)');
define('SQL_INSERT_GENERIC', 'INSERT INTO %t (%c) VALUES (%v)');
define('SQL_REPLACE_GENERIC', 'REPLACE INTO %t (%c) VALUES (%v)');
define('SQL_DELETE_GENERIC', 'DELETE IGNORE FROM %t WHERE %c');
define('SQL_UPDATE_GENERIC', 'UPDATE %t SET %ufv WHERE %cfv');
define('SQL_DROP_TABLE_GENERIC', 'DROP TABLE IF EXISTS %tn ');


/**
 * Class DbPdo
 * a PDO wrapper for Mysql connections
 */
class DbPdo
{
    // Private properties
    private static $db = null; // connection instance
    private static $connection_info = null; // connection credentials
    private static $query = null;
    
    public static $results = array();
    
    /**
     * Connect method
     *
     * @param string|null $db_type [sqlite, mysql]
     */
    private static function connect($db_type) {
        
        if(!self::$db) {
             // Get the connection info from the .ini file
            self::$connection_info = parse_ini_file(MYSQL_CREDENTIALS_FILE, true);
            
            // Create the database connection with attributes
            switch ($db_type) {
                case 'sqlite':
                    self::$db = new PDO('sqlite:' . self::$connection_info['db']['database'] . '.sqlite3');
                    break;
                case 'mysql':
                default:
                    $connectionString = 'mysql:host=' . self::$connection_info['db']['hostname'] . ';dbname=' . self::$connection_info['db']['database'] . ';charset=utf8';
                    self::$db = new PDO($connectionString, self::$connection_info['db']['username'], self::$connection_info['db']['password']);
                    break;
            }
            
            self::$db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            self::$db->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
        }
        
        return self::$db;
    }
    
    /**
     * Performs the Query statement, substituting ? for values in $params
     *
     * @param string $statement  SQL DEFINE statement
     * @param array  $params     Array of values to translate into the string
     * @param int    $iterations Keep track of reinstantiation attempts, max 1 recursion
     *
     * @return array Array of 0 or More results
     */
    public static function query($statement, $params = array(), $iterations = 0)
    {
        self::connect();
        
        try {
            self::$query = self::$db->prepare($statement);
            self::$query->execute($params);
            self::$results = self::$query->fetchAll(PDO::FETCH_ASSOC);
        } catch (PDOException $e) {
            error_log($e->getMessage()."\nSQL Statement: ".$statement);
        }
        return self::$results;
    }
    
    /**
     * Executes REPLACE statement on TABLE with Field-Value pairs
     * Was originally called ->update, but SQL UPDATE is handled differently than INSERT/REPLACE
     * So they were split and abstracted
     *
     * @param string $table    DB TABLE DEFINE
     * @param array  $fv_pairs Array of field => value pairs, only one dimensional
     *                         $fv_pairs = array('userid' => 123, 'name' => 'Billy');
     *
     * @return bool|int Last Inserted ID or false
     */
    public static function insert($table, $fv_pairs)
    {
        return self::_modifySingle(SQL_INSERT_GENERIC, $table, $fv_pairs);
    }
    
    /**
     * Executes REPLACE statement on TABLE with Field-Value pairs
     * Was originally called ->update, but SQL UPDATE is handled differently than INSERT/REPLACE
     * So they were split and abstracted
     *
     * @param string $table    DB TABLE DEFINE
     * @param array  $fv_pairs Array of field => value pairs, only one dimensional
     *                         $fv_pairs = array('userid' => 123, 'name' => 'Billy');
     *
     * @return bool|int Last Inserted ID or false
     */
    public static function replace($table, $fv_pairs)
    {
        return self::_modifySingle(SQL_REPLACE_GENERIC, $table, $fv_pairs);
    }
    
    /**
     * Executes an UPDATE statement on TABLE with field-value pairs and criteria-pairs
     *
     * @param string $table          DB TABLE DEFINE
     * @param array  $fv_pairs       Array of field => value pairs, only ONE dimensional
     *                               $updates = array('name' => 'Billy-Bob');
     * @param array  $criteria_pairs Array of [field, op, value] arrays, TWO dimensional
     *                               $criteria_pairs[] = array('field' => 'userid', 'op' => '=', 'value' => 123);
     * @param int    $iterations     Recursion counter
     *
     * @return bool|string
     */
    public static function update($table, $fv_pairs, $criteria_pairs, $iterations = 0)
    {
        $sql = SQL_UPDATE_GENERIC;
        /*
        * Basically we need to run two variable variable construction loops
        * 1.) for the UPDATE pairing
        * 2.) for the CRITERIA pairing
        *
        * Iterate over the SET field-value pairs and build arrays for implosion and assignment
        * Then do the same for the CRITERIA field-op-value sets
        *
        * Then implode the update/criteria_str arrays into the statement with appropriate concatenation
        * Then iterate the *_value_str arrays to bindParam with variable variables for each field
        * Then assign the variable variables the value for that field from the original pairing array
        *
        * Execute in a loop, and commit the transaction.
        */
        $u_value_str = $update_str = $criteria_str = array();
        foreach ($fv_pairs as $f => $v) {
            $u_value_str[$f] = ':' . $f;
            $update_str[$f] = $f . '=:' . $f;
        }
        
        $c_value_str = $criteria_str = array();
        foreach ($criteria_pairs as $criteria) {
            $f = $criteria['field'];
            $o = $criteria['op'];
            $c_value_str[$f . '_c'] = ':' . $f . '_c';
            $criteria_str[$f . '_c'] = $f . $o . ':' . $f . '_c';
        }
        
        try {
            // Start the transaction
            self::$db->beginTransaction();
            // Prepare the SQL statement
            $sql = strtr($sql, array(
                '%t'   => $table,
                '%ufv' => implode(', ', array_unique($update_str)),
                '%cfv' => implode(' AND ', array_unique($criteria_str))
            ));
            
            self::$query = self::$db->prepare($sql);
            
            // loop through to bind the variable variable name for each field
            foreach ($u_value_str as $f => $v) {
                self::$query->bindParam($v, $$f); // Intentional variable variable
            }
            
            foreach ($c_value_str as $f => $v) {
                self::$query->bindParam($v, $$f); // Intentional variable variable
            }
            
            // loop through to assign the variable variable's value
            foreach ($fv_pairs as $f => $v) {
                $$f = $v; // set the variable variable
            }
            
            foreach ($criteria_pairs as $criteria) {
                $f = $criteria['field'] . '_c';
                $$f = $criteria['value']; // set the variable variable
            }
            
            self::$query->execute(); // execute the replace query
            // attempt to commit the transaction
            self::$db->commit();
            // return # rows affected
            self::$results = self::$db->lastInsertId();
        } catch (PDOException $e) {
            self::$db->rollBack();
            error_log($e->getMessage());
        }
        
        return self::$results;
    }
    
    /**
     * Executes delete statement with field-value pairs criteria
     *
     * @param string $table      DB TABLE DEFINE
     * @param array  $fv_pairs   Array of field => value pairs, only one dimensional
     *                           $fv_pairs = array('userid' => 123, 'name' => 'Billy');
     * @param int    $iterations Recursion counter
     *
     * @return bool Success/Fail
     */
    public static function delete($table, $fv_pairs, $iterations = 0)
    {
        $sql = SQL_DELETE_GENERIC;
        // Build the field assignment strings
        $field_str = $value_str = array();
        $criteria = array();
        foreach ($fv_pairs as $f => $v) {
            $criteria[$f] = $f . '=:' . $f;
            $field_str[$f] = $f;
            $value_str[$f] = ':' . $f;
        }
        try {
            // Start the transaction
            self::$db->beginTransaction();
            // Prepare the SQL statement
            $sql = strtr($sql, array(
                '%t' => $table,
                '%c' => implode(' AND ', array_unique($criteria))
            ));
            self::$query = self::$db->prepare($sql);
            // loop through to bind the variable variable name for each field
            foreach ($value_str as $f => $v) {
                self::$query->bindParam($v, $$f); // Intentional variable variable
            }
            // loop through to assign the variable variable's value, and execute
            foreach ($fv_pairs as $f => $v) {
                $$f = $v; // set the variable variable
            }
            self::$query->execute(); // execute the delete query
            // attempt to commit the transaction
            self::$results = self::$db->commit();
        } catch (PDOException $e) {
            self::$db->rollBack();
            error_log($e->getMessage());
        }
        return self::$results;
    }
    
    /**
     * Drops a table from the database
     *
     * @param string $table_name Table name to drop
     *
     * @return array|bool
     */
    public static function drop($table_name)
    {
        $sql = strtr(SQL_DROP_TABLE_GENERIC, array('%tn' => $table_name));
        try {
            // Start the transaction
            self::$db->beginTransaction();
            self::$query = self::$db->prepare($sql);
            self::$query->execute(); // execute the drop query
            self::$results = self::$db->commit();
        } catch (PDOException $e) {
            self::$db->rollBack();
            error_log($e->getMessage());
        }
        return self::$results;
    }
    
    /**
     * Executes a Create Table statement in a transaction, with table_name and an array of column=>type pairs
     *
     * @param string $table_name
     * @param array  $columns_types Array of Column_Name => Type pairs (one dimensional)
     *
     * @return array|bool Success or false
     */
    public static function createTable($table_name, $columns_types)
    {
        $sql = SQL_CREATE_TABLE_GENERIC;
        $column_str = array();
        foreach ($columns_types as $column => $type) {
            $column_str[] = $column . ' ' . $type;
        }
        $sql = strtr($sql, array(
            '%tn' => $table_name,
            '%ct' => implode(', ', $column_str)
        ));
        try {
            self::$db->beginTransaction();
            self::$query = self::$db->prepare($sql);
            self::$query->execute();
            self::$results = self::$db->commit();
        } catch (PDOException $e) {
            // Attempt to recreate the PDO
            self::$db->rollBack();
            error_log($e->getMessage());
        }
        return self::$results;
    }
    
    /**
     * Executes multiple REPLACE statements in a transaction on TABLE with an Array of Field=>Value pair Arrays
     *
     * @param string $table          DB TABLE DEFINE
     * @param array  $fv_pairs_array Array of Field => Value pair arrays (two dimensional)
     *                               $fv_pairs_array[] = array('userid' => 123, 'name' => 'Billy');
     *                               $fv_pairs_array[] = array('userid' => 456, 'name' => 'Lucy');
     *
     * @return bool|int Last Modified Row ID or false
     */
    public static function replaceMultiple($table, $fv_pairs_array)
    {
        return self::_modifyMultiple(SQL_REPLACE_GENERIC, $table, $fv_pairs_array);
    }
    
    /**
     * Executes multiple INSERT statements in a transaction on TABLE with an Array of Field=>Value pair Arrays
     *
     * @param string $table          DB TABLE DEFINE
     * @param array  $fv_pairs_array Array of Field => Value pair arrays (two dimensional)
     *                               $fv_pairs_array[] = array('userid' => 123, 'name' => 'Billy');
     *                               $fv_pairs_array[] = array('userid' => 456, 'name' => 'Lucy');
     *
     * @return bool|int Last Modified Row ID or false
     */
    public static function insertMultiple($table, $fv_pairs_array)
    {
        return self::_modifyMultiple(SQL_INSERT_GENERIC, $table, $fv_pairs_array);
    }
    
    /**
     * Gets the next top-element from the results array of a query
     * @return array|mixed Value of the next top-element in the results array
     */
    public static function getNext()
    {
        return (!empty(self::$results)) ? array_shift(self::$results) : array();
    }
    
    /**
     * Abstraction of the INSERT/REPLACE methods, since they are
     * functionally the same, except for the SQL statement
     * Performs the SQL statement with field-value pairs in a single-execute transaction
     *
     * @param string $sql        SQL DEFINE statement
     * @param string $table      DB TABLE DEFINE
     * @param array  $fv_pairs   Array of field => value pairs, only one dimensional
     *                           $fv_pairs = array('userid' => 123, 'name' => 'Billy');
     * @param int    $iterations Recursion counter
     *
     * @return bool Result
     */
    private static function _modifySingle($sql, $table, $fv_pairs, $iterations = 0)
    {
        // Build the field assignment strings
        $field_str = $value_str = array();
        foreach ($fv_pairs as $f => $v) {
            $field_str[$f] = $f;
            $value_str[$f] = ':' . $f;
        }
        self::$results = false;
        try {
            // Start the transaction
            self::$db->beginTransaction();
            // Prepare the SQL statement
            $sql = strtr($sql, array(
                '%t' => $table,
                '%c' => implode(', ', array_unique($field_str)),
                '%v' => implode(', ', array_unique($value_str))
            ));
            self::$query = self::$db->prepare($sql);
            // loop through to bind the variable variable name for each field
            foreach ($value_str as $f => $v) {
                self::$query->bindParam($v, $$f); // Intentional variable variable
            }
            // loop through to assign the variable variable's value, and execute
            foreach ($fv_pairs as $f => $v) {
                $$f = $v; // set the variable variable
            }
            self::$query->execute(); // execute the replace query
            // attempt to commit the transaction
            self::$db->commit();
            self::$results = true;
        } catch (PDOException $e) {
            self::$db->rollBack();
            echo $e->getMessage();
        }
        return self::$results;
    }
    
    private static function _modifyMultiple($sql, $table, $fv_pairs_array, $iterations = 0)
    {
        // Build the field assignment strings
        $field_str = $value_str = array();
        foreach ($fv_pairs_array as $fv_pairs) {
            foreach ($fv_pairs as $f => $v) {
                $field_str[$f] = $f;
                $value_str[$f] = ':' . $f;
            }
        }
        self::$results = 0;
        try {
            // Start the transaction
            self::$db->beginTransaction();
            // Prepare the SQL statement
            $sql = strtr($sql, array(
                '%t' => $table,
                '%c' => implode(', ', array_unique($field_str)),
                '%v' => implode(', ', array_unique($value_str))
            ));
            self::$query = self::$db->prepare($sql);
            // loop through to bind the variable variable name for each field
            foreach ($value_str as $f => $v) {
                self::$query->bindParam($v, $$f); // Intentional variable variable
            }
            // loop through to assign the variable variable's value, and execute
            foreach ($fv_pairs_array as $fv_pairs) {
                foreach ($fv_pairs as $f => $v) {
                    $$f = $v; // set the variable variable
                }
                self::$query->execute(); // execute the replace query
                self::$results += self::$query->rowCount(); // return # rows affected
            }
            // attempt to commit the transaction
            self::$db->commit();
        } catch (PDOException $e) {
            self::$db->rollBack();
            echo $e->getMessage();
        }
        return self::$results;
    }
}