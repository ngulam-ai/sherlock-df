package agency.akcom.mmg.sherlock.df;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;

public class TableRowCreator {
	private String[] tmpSchemaRow;
	private JSONObject elementJSON;
	private String element;

	private static final Logger LOG = LoggerFactory.getLogger(TableRowCreator.class);

	public TableRowCreator(String element) {
		this.element = element;
		elementJSON = new JSONObject(element);		
	}

	public TableRow getTableRow() {		
		TableRow tableRow = setFields(DataflowUtils.SCHEMA_WITH_PARAMS.iterator(), null, new TableRow());
		tableRow.set("tmp_raw_request_json", element);
		return tableRow;
	}

	private TableRow setFields(Iterator<String[]> schemaIterator, String recordName, TableRow tableRow) {
		// LOG.info(String.join(",", tmpSchemaRow));

		String[] schemaRow;
		while (schemaIterator.hasNext()) {
			if (tmpSchemaRow != null) {
				schemaRow = tmpSchemaRow;
				tmpSchemaRow = null;
			} else {
				schemaRow = schemaIterator.next().clone();
				if (recordName != null) {
					if (schemaRow[0].startsWith(recordName + ".")) {
						schemaRow[0] = schemaRow[0].replaceFirst(recordName + ".", "");
					} else {
						tmpSchemaRow = schemaRow;
						return tableRow;
					}
				}
			}
			tableRow.set(schemaRow[0], getNextFieldValue(schemaIterator, schemaRow));
			//LOG.info(tableRow.toString());
		}

		return tableRow;
	}

	private Object getNextFieldValue(Iterator<String[]> schemaIterator, String[] schemaRow) {
		// LOG.info(String.join(",", schemaRow));

		if ("REPEATED".equals(schemaRow[2])) {
			// TODO use stub for now since we only have one repeated "customDimensions"
			TableRow[] reapetedTableRows = createRepeated(schemaIterator, schemaRow);
			LOG.debug("REPEATED:" + schemaRow[0] + ": " + reapetedTableRows);
			return reapetedTableRows;
			
		} else if ("RECORD".equals(schemaRow[1])) {
			TableRow tmpTableRow = setFields(schemaIterator, schemaRow[0], new TableRow());
			LOG.debug("RECORD:" + schemaRow[0] + ": " + tmpTableRow.toString());
			return tmpTableRow;
			
		} else {
			Object value = null;
			
			String[] keys = schemaRow[3].split(",");
			// TOOD process more than One key separated by ','
			for (int i = 0; i < keys.length; i++) {	
				String key = keys[i].trim();
				if (!key.isEmpty()) {
					try {
						value = elementJSON.get(key);
						break; //try until first found						
					} catch (JSONException e) {
						LOG.info(e.getMessage());
					}
				}				
			}			

			// --- fix for:
			// Insert failed:
			// [{"errors":[{"debugInfo":"","location":"trackingid","message":"Array
			// specified for non-repeated field.","reason":"invalid"}],"index":0}]
			if ("trackingId".equals(schemaRow[0])) {
				try {
					value = elementJSON.getString(keys[0]);
				} catch (JSONException e) {
					LOG.warn(e.getMessage());
					value = "";
				}
			}
			// ---

			//LOG.info(schemaRow[0] + ": " + value + " (" + key + ")");

			return value;
		}
	}

	private TableRow[] createRepeated(Iterator<String[]> schemaIterator, String[] schemaRow) {
		List<TableRow> repeatedRows = new ArrayList<>();
		for (int i = 1; i < 99; i++) {
			String key = schemaRow[3] + i;
			if (elementJSON.has(key)) {
				Object value = elementJSON.get(key);
				TableRow tableRow = new TableRow();
				tableRow.set("index", i);
				tableRow.set("value", value);
				repeatedRows.add(tableRow);
			} 			
		}
		
		schemaIterator.next();
		schemaIterator.next();
		
		return (TableRow[]) repeatedRows.toArray(new TableRow[0]);
	}
}
