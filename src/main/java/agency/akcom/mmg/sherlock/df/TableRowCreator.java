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
			LOG.info(tableRow.toString());
		}

		return tableRow;
	}

	private Object getNextFieldValue(Iterator<String[]> schemaIterator, String[] schemaRow) {
		// LOG.info(String.join(",", schemaRow));
		
		if ("REPEATED".equals(schemaRow[2])) {
			//TODO use stub for now since we only have one repeated "customDimensions"
			TableRow[] reapetedTableRows = createRepeated(schemaIterator, schemaRow);
			LOG.info("REPEATED:" + schemaRow[0] + ": " + reapetedTableRows);
			return reapetedTableRows;
			
		} else if ("RECORD".equals(schemaRow[1])) {
			TableRow tmpTableRow = setFields(schemaIterator, schemaRow[0], new TableRow());
			LOG.info("RECORD:" + schemaRow[0] + ": " + tmpTableRow.toString());
			return tmpTableRow;
			
		} else {
			String key = schemaRow[3];
			Object value = null;
			if (!key.isEmpty()) {
				try {
					value = elementJSON.get(key);
				} catch (JSONException e) {
					LOG.warn(e.getMessage());
				}
			}
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
