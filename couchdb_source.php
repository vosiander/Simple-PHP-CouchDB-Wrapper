<?php
/**
 * 
 * Copyright (c) 2010 Veit Osiander
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * 
 * @author Veit Osiander
 * @copyright Veit Osiander
 * @license MIT license http://www.opensource.org/licenses/mit-license.php
 *
 */
class CouchdbSource {
	private $socket = null;
	private $url = 'http://user:password@127.0.0.1:5984';
	private $isCurlAvailable = true;
	private $findCount = 0; // find count
	private $lastError = false;
	
	/**
	 * 
	 * Constructor to init dataspurce socket connection
	 * @param array $config
	 */
	public function __construct($config = array()) {
		$this->url = isset($config['host']) ? $config['host'] : $this->url; // use default
		
		if (!function_exists('curl_init')){ 
	        $this->isCurlAvailable = false;
	    } else {
	    	$this->socket = curl_init();
	    }
	}
	
	/**
	 * 
	 * Desctructor to close socket connection
	 */
	public function __destruct() {
		if($this->isCurlAvailable) {
			curl_close( $this->socket );
		}
	}
	
	/**
	 * 
	 * Standard function to request data from the database 
	 * 
	 * @param string $method
	 * @param string $query
	 * @param array $data
	 */
	public function request($method, $query, $data = false) {
		// remove first "/"
		if(substr($query, 0, 1) == '/') {
			$query = substr($query, 1);
		}
		$method = strtoupper($method);
		if(!$this->isCurlAvailable) {
			return false;
		}
		if(!in_array($method, array('POST', 'PUT', 'DELETE', 'GET'))) {
			return false;
		}
		
		$options = array(
	        CURLOPT_RETURNTRANSFER 	=> true,     // return web page
	        CURLOPT_HEADER         	=> false,    // don't return headers
	        CURLOPT_FOLLOWLOCATION 	=> true,     // follow redirects
	        CURLOPT_ENCODING       	=> "utf-8",       // handle all encodings
	        CURLOPT_AUTOREFERER    	=> true,     // set referer on redirect
	        CURLOPT_CONNECTTIMEOUT 	=> 120,      // timeout on connect
	        CURLOPT_TIMEOUT        	=> 120,      // timeout on response
	        CURLOPT_MAXREDIRS      	=> 3,       // stop after 10 redirects
	        CURLOPT_URL			   	=> $this->url.'/'.$query,
	        CURLOPT_CUSTOMREQUEST	=> $method
	    );
	    
	    // add post data if POST is set as Method
	    if(!empty($data) && ($method == 'POST' || $method == 'PUT')) {
	    	$options[CURLOPT_POSTFIELDS] = json_encode($data);
	    	$options[CURLOPT_HTTPHEADER] = array (
		        "Content-Type: application/json"
		    );
	    }
	
	    curl_setopt_array( $this->socket, $options );
	    $content = curl_exec( $this->socket );
	    $err     = curl_errno( $this->socket );
	    $errmsg  = curl_error( $this->socket );
	    $header  = curl_getinfo( $this->socket );
	
	    $header['errno']   = $err;
	    $header['errmsg']  = $errmsg;
	    $header['content'] = $content;
	    
	    $return_array = json_decode($content, true);
	    if(isset($return_array['error'])) {
	    	$this->lastError = $return_array;
	    }
	    
	    return !empty($err) ? false : $return_array;
	}
	
	/**
	 * 
	 * Returns the last Error
	 * 
	 * @return array | false
	 */
	public function lastError() {
		return $this->lastError;
	}
	
	/**
	 * 
	 * Get last find count
	 */
	public function getLastFindCount() {
		return $this->findCount;
	}
	
	/**
	 * Get one Uuid
	 */
	public function getUuid() {
		$response = $this->get('/_uuids');
		if(!isset($response['error'])) {
			return $response['uuids'][0];
		}
		return false;
	}
	
	/**
	 * Actions
	 */
	
	/**
	 * 
	 * Check if the current database/view/document exists
	 * @param unknown_type $query
	 */
	public function exists($query) {
		$response = $this->request('GET', $query);
		if(isset($response['error'])) {
			return false;
		}
		return true;
	}
	
	/**
	 * 
	 * creates a database
	 * @param unknown_type $name
	 */
	public function createDatabase($name) {
		$response = $this->request('PUT', '/'.$name);
		return $response;
	}
	
	/**
	 * 
	 * GET request
	 * @param string $query
	 */
	public function get($query) {
		$response = $this->request('GET', $query);
		return $response;
	}
	
	/**
	 * 
	 * Searches the database
	 * 
	 * @param string $query
	 * @param array $options
	 */
	public function find($query, $options = array()) {
		if(!empty($options)) {
			$get_parameters = array();
			
			foreach($options as $key => $value) {
				if($value === true) $value = "true";
				if($value === false) $value = "false";
				$get_parameters[] = $key .'='. $value;
			}
			
			$query .= '?'.implode('&', $get_parameters);
		}
		$response = $this->get($query);
		
		if(isset($response->error)) {
			// TODO: set error
			return false;
		} else {
			// found just one element
			if(isset($response['_id'])) {
				$this->findCount = 1;
				return $response;	
			} 
			// found n rows
			if(isset($response['rows'])) {
				$this->findCount = isset($response['total_rows']) ? $response['total_rows'] : count($response['rows']);
				return $response['rows'];
			}
			return false;
		}
	}
	
	/**
	 * 
	 * Insert an document in a database
	 * 
	 * @param string $database
	 * @param array $data
	 */
	public function insertDocument($database, $data) {
		if(!isset($data['_id'])) {
			// get uuid
			$uuid = $this->getUuid();
			$data['_id'] = $uuid;
		}
		// store data in Database
		$response = $this->request('PUT', '/'.$database.'/'.$data['_id'], $data);
		return isset($response['ok']) && $response['ok'] ? $response['id'] : false;
	}
	
	/**
	 * 
	 * Insert a View in a database
	 * 
	 * @param string $database
	 * @param string $name
	 * @param array $views
	 */
	public function insertView($database, $name, $views) {
		$data = array(
			'_id' => '_design/'.$name,
			'views' => $views
		);
		$response = $this->insertDocument($database, $data);
		if(isset($response['error'])) {
			return false;
		} else {
			return $response;
		}
	}
	
	public function delete($database, $id) {
		// get the element to use latest revision
		$element = $this->get('/'.$database.'/'.$id);
		if(isset($element['error'])) {
			return false;
		}
		$response = $this->request('DELETE', '/'.$database.'/'.$id.'?rev='.$element['_rev']);
		return (isset($response['error'])) ? false : true;
	}
	
	/**
	 * 
	 * Checks if the couchdb server response with "Welcome"
	 * @return bool
	 */
	public function isConnected() {
		$response = $this->request('GET', '');
		return (!empty($response) && $response['couchdb'] == 'Welcome') ? true : false;
	}
	
	/**
	 * 
	 * Returns a view function with key and values
	 * @param string $database
	 * @param string $document_view
	 * @param string $map_function
	 * @param array $options
	 */
	public function findReduce($database, $document_view, $map_function, $options = array()) {
		$list = $this->find('/'.$database.'/_design/'.$document_view.'/_view/'.$map_function, $options);
		$return_list = array();
		if(!isset($list['error'])) {
			foreach($list as $element) {
				if(is_array($element['key']) || is_object($element['key'])) {
					break; // break because key is not a string or int
				}
				$return_list[$element['key']] = $element['value'];
			}
		}
		return $return_list;
	}
}