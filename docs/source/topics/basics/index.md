# Basics

## Datatypes Mapping

<table class="table table-striped table-hover table-condensed">
  <thead>
  <tr>
   <th>ScyllaDB/Cassandra Type(s)</th>
   <th>Driver Type</th>
  </tr>
  </thead>

  <tbody>
  <tr>
   <td><code>int</code></td>
   <td><code>cass_int32_t</code></td>
  </tr>
  <tr>
   <td><code>bigint</code>, <code>counter</code>, <code>timestamp</code></td>
   <td><code>cass_int64_t</code></td>
  </tr>
  <tr>
   <td><code>float</code></td>
   <td><code>cass_float_t</code></td>
  </tr>
  <tr>
   <td><code>double</code></td>
   <td><code>cass_double_t</code></td>
  </tr>
  <tr>
   <td><code>boolean</code></td>
   <td><code>cass_bool_t</code></td>
  </tr>
  <tr>
   <td><code>ascii</code>, <code>text</code>, <code>varchar</code></td>
   <td><code>const char&#42;</code></td>
  </tr>
  <tr>
   <td><code>blob</code>, <code>varint</code></td>
   <td><code>const cass_byte_t&#42;</code></td>
  </tr>
  <tr>
   <td><code>uuid</code>, <code>timeuuid</code></td>
   <td><code>CassUuid</code></td>
  </tr>
  <tr>
   <td><code>inet</code></td>
   <td><code>CassInet</code></td>
  </tr>
  <tr>
   <td><code>decimal</code></td>
   <td><code>const cass_byte_t&#42; (varint) and a cass_int32_t (scale)</code></td>
  </tr>
  <tr>
   <td><code>list</code>, <code>map</code>, <code>set</code></td>
   <td><code>CassCollection</code></td>
  </tr>
  <tr>
   <td><code>tuple</code></td>
   <td><code>CassTuple</code></td>
  </tr>
  <tr>
   <td><code>user-defined type</code></td>
   <td><code>CassUserType</code></td>
  </tr>
  <tr>
   <td><code>tinyint</code></td>
   <td><code>cass_int8_t</code></td>
  </tr>
  <tr>
   <td><code>smallint</code></td>
   <td><code>cass_int16_t</code></td>
  </tr>
  <tr>
   <td><code>date</code></td>
   <td><code>cass_uint32_t</code></td>
  </tr>
  <tr>
   <td><code>time</code></td>
   <td><code>cass_int64_t</code></td>
  </tr>
  </tbody>
</table>

```{eval-rst}
.. toctree::
  :hidden:
  :glob:

  batches
  binding-parameters
  client-side-timestamps
  consistency
  data-types
  date-and-time
  futures
  handling-results
  keyspaces
  prepared-statements
  schema-metadata
  tuples
  user-defined-types
  uuids
```
