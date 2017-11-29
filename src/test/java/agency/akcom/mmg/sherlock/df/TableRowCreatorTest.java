package agency.akcom.mmg.sherlock.df;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.api.services.bigquery.model.TableRow;

@RunWith(JUnit4.class)
public class TableRowCreatorTest {

	private static final String HIT_JSON = "{" + "	'gtm': 'GbeN4CPW24'," + "	'cd78': '$$CUSTOM_PARAM(cd78)$$',"
			+ "	'cd79': '2145358953.1511857872_1511878959435'," + "	'cd76': '$$CUSTOM_PARAM(cd76)$$',"
			+ "	'_gid': '399233302.1511857872'," + "	'cd77': '$$CUSTOM_PARAM(cd77)$$',"
			+ "	'pr1nm': '4039222 - Adult Smartlink Global (Adult)',"
			+ "	'cd80': 'MjEjMjMyIzE2IzU4MHw4Mzl8UlV8M3wxfHx8d2J2MzRwOTI5ajc2fDdjZWQzOTYwLWQ0MTYtMTFlNy1hZjY1LWQ0ODU2NGM2MmY0NHx8',"
			+ "	'je': '0'," + "	'cd29': '232'," + "	'cd27': '$$CUSTOM_PARAM(cd27)$$'," + "	'cd28': 'Chrome',"
			+ "	'cd25': '16'," + "	'cd26': '$$CUSTOM_PARAM(cd26)$$'," + "	'cd23': '$$CUSTOM_PARAM(cd23)$$',"
			+ "	'cd24': '$$CUSTOM_PARAM(cd24)$$'," + "	'cd21': '$$CUSTOM_PARAM(cd21)$$',"
			+ "	'cd22': '$$CUSTOM_PARAM(cd22)$$'," + "	'cd30': 'Yaroslavl'," + "	'sd': '24-bit',"
			+ "	'uid': 'Russian Federation;Yaroslavskaya oblast;Yaroslavl;unknown;unknown;Windows;10;Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3269.3 Safari/537.36;94.25.239.174;$$CUSTOM_PARAM(cd11)$$;$$CUSTOM_PARAM(cd7)$$;$$CUSTOM_PARAM(cd10)$$',"
			+ "	'cd31': '$$CUSTOM_PARAM(cd31)$$'," + "	'pr1ca': 'Smartlink'," + "	'sr': '2560x1080',"
			+ "	'cc': '$$CUSTOM_PARAM(cc)$$'," + "	'a': '390101226'," + "	'cd18': '1511878959396.vkx2qjja',"
			+ "	'cd16': '$$CUSTOM_PARAM(cd16)$$'," + "	'cd17': '$$CUSTOM_PARAM(cd17)$$'," + "	'cm': 'Blind',"
			+ "	'cd14': '$$CUSTOM_PARAM(cd14)$$'," + "	'cn': 'Mondia Media|Smartlink (Adult)_copy',"
			+ "	'cd15': 'Blind'," + "	'cd12': 'Mondia%20Media'," + "	'cd13': '$$CUSTOM_PARAM(cd13)$$',"
			+ "	'cd10': '$$CUSTOM_PARAM(cd10)$$'," + "	'cd11': '2145358953.1511857872'," + "	'cs': 'Mondia Media',"
			+ "	'cd20': '$$CUSTOM_PARAM(cd20)$$'," + "	't': 'event'," + "	'v': '1'," + "	'z': '1379155459',"
			+ "	'de': 'UTF-8'," + "	'dl': 'http://n38adshostnet.com/ads?key=b7e41cd81d999242866d54770553fce6',"
			+ "	'cd49': '$$CUSTOM_PARAM(cd49)$$'," + "	'cd47': '$$CUSTOM_PARAM(cd47)$$',"
			+ "	'cd48': '94.25.239.174',"
			+ "	'cd45': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',"
			+ "	'cd46': 'en-US,en;q=0.9'," + "	'tid': 'UA-98835143-2'," + "	'cd43': '$$CUSTOM_PARAM(cd43)$$',"
			+ "	'cd44': '$$CUSTOM_PARAM(cd44)$$'," + "	'cd52': '57.62987'," + "	'cd53': '$$CUSTOM_PARAM(cd53)$$',"
			+ "	'cd50': 'en'," + "	'cd51': '$$CUSTOM_PARAM(cd51)$$'," + "	'pr1id': '232'," + "	'ul': 'en-us',"
			+ "	'ea': 'Click'," + "	'ec': 'AdClick'," + "	'pr1br': 'Avazu (AD)',"
			+ "	'cd38': '$$CUSTOM_PARAM(cd38)$$'," + "	'cd39': '$$CUSTOM_PARAM(cd39)$$',"
			+ "	'cd36': '$$CUSTOM_PARAM(cd36)$$'," + "	'cd37': '$$CUSTOM_PARAM(cd37)$$',"
			+ "	'cd34': '$$CUSTOM_PARAM(cd34)$$'," + "	'cd35': 'unknown',"
			+ "	'cd32': '7ced3960-d416-11e7-af65-d48564c62f44'," + "	'cd33': 'Russian Federation',"
			+ "	'cd41': '$$CUSTOM_PARAM(cd41)$$'," + "	'cd42': '$$CUSTOM_PARAM(cd42)$$',"
			+ "	'cd40': '$$CUSTOM_PARAM(cd40)$$'," + "	'vp': '2420x960'," + "	'ni': '0',"
			+ "	'cid': '2145358953.1511857872'," + "	'cd69': 'Yaroslavskaya oblast',"
			+ "	'cd68': '$$CUSTOM_PARAM(cd68)$$'," + "	'cd65': '$$CUSTOM_PARAM(cd65)$$',"
			+ "	'cd74': '$$CUSTOM_PARAM(cd74)$$',"
			+ "	'cd75': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3269.3 Safari/537.36',"
			+ "	'cd72': '$$CUSTOM_PARAM(cd72)$$'," + "	'cd73': '1511878958962',"
			+ "	'cd70': '$$CUSTOM_PARAM(cd70)$$'," + "	'cd71': '$$CUSTOM_PARAM(cd71)$$',"
			+ "	'hitId': '1bfd4660-7113-4b49-8fe5-30027d16477c'," + "	'cd58': '10',"
			+ "	'cd59': '$$CUSTOM_PARAM(cd59)$$'," + "	'cd56': 'Windows'," + "	'cd57': 'unknown',"
			+ "	'cd54': '39.87368'," + "	'cd55': '$$CUSTOM_PARAM(cd55)$$'," + "	'cd2': 'Avazu (AD)',"
			+ "	'cd63': 'http',"
			+ "	'cd1': 'Russian Federation;Yaroslavskaya oblast;Yaroslavl;unknown;unknown;Windows;10;Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3269.3 Safari/537.36;94.25.239.174;$$CUSTOM_PARAM(cd11)$$;$$CUSTOM_PARAM(cd7)$$;$$CUSTOM_PARAM(cd10)$$',"
			+ "	'cd64': '$$CUSTOM_PARAM(cd64)$$'," + "	'pa': 'click'," + "	'cd4': '$$CUSTOM_PARAM(cd4)$$',"
			+ "	'cd61': '580'," + "	'cd3': '4039222 - Adult Smartlink Global (Adult)',"
			+ "	'cd62': 'Smartlink (Adult)_copy'," + "	'_s': '1',"
			+ "	'cd6': 'MjEjMjMyIzE2IzU4MHw4Mzl8UlV8M3wxfHx8d2J2MzRwOTI5ajc2fDdjZWQzOTYwLWQ0MTYtMTFlNy1hZjY1LWQ0ODU2NGM2MmY0NHx8',"
			+ "	'cd5': '$$CUSTOM_PARAM(cd5)$$'," + "	'cd60': '$$CUSTOM_PARAM(cd60)$$'," + "	'_u': 'SCCAAEAL~',"
			+ "	'cd8': '$$CUSTOM_PARAM(cd8)$$'," + "	'_v': 'j66'," + "	'cd7': '$$CUSTOM_PARAM(cd7)$$',"
			+ "	'cd9': '$$CUSTOM_PARAM(cd9)$$'," + "	'time': '1511878959416'" + "}" + "";
	private static final String TABLE_ROW_JSON = "{hitId=1bfd4660-7113-4b49-8fe5-30027d16477c, userId=Russian Federation;Yaroslavskaya oblast;Yaroslavl;unknown;unknown;Windows;10;Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3269.3 Safari/537.36;94.25.239.174;$$CUSTOM_PARAM(cd11)$$;$$CUSTOM_PARAM(cd7)$$;$$CUSTOM_PARAM(cd10)$$, userPhone=null, userEmail=null, clientId=null, trackingId=UA-98835143-2, date=null, traffic={referralPath=null, campaign=null, source=null, medium=null, keyword=null, adContent=null, campaignId=null, gclid=null, dclid=null}, device={ip=null, userAgent=null, flashVersion=null, language=null, screenColors=24-bit, screenResolution=2560x1080}, geo={id=null}, hour=null, minute=null, time=1511878959416, queueTime=null, isSecure=null, currency=null, referer=null, dataSource=null, type=event, eCommerceAction={action_type=click, option=null, step=null, list=null}, experiment={experimentId=null, experimentVariant=null}, tmp_raw_request_json={	'gtm': 'GbeN4CPW24',	'cd78': '$$CUSTOM_PARAM(cd78)$$',	'cd79': '2145358953.1511857872_1511878959435',	'cd76': '$$CUSTOM_PARAM(cd76)$$',	'_gid': '399233302.1511857872',	'cd77': '$$CUSTOM_PARAM(cd77)$$',	'pr1nm': '4039222 - Adult Smartlink Global (Adult)',	'cd80': 'MjEjMjMyIzE2IzU4MHw4Mzl8UlV8M3wxfHx8d2J2MzRwOTI5ajc2fDdjZWQzOTYwLWQ0MTYtMTFlNy1hZjY1LWQ0ODU2NGM2MmY0NHx8',	'je': '0',	'cd29': '232',	'cd27': '$$CUSTOM_PARAM(cd27)$$',	'cd28': 'Chrome',	'cd25': '16',	'cd26': '$$CUSTOM_PARAM(cd26)$$',	'cd23': '$$CUSTOM_PARAM(cd23)$$',	'cd24': '$$CUSTOM_PARAM(cd24)$$',	'cd21': '$$CUSTOM_PARAM(cd21)$$',	'cd22': '$$CUSTOM_PARAM(cd22)$$',	'cd30': 'Yaroslavl',	'sd': '24-bit',	'uid': 'Russian Federation;Yaroslavskaya oblast;Yaroslavl;unknown;unknown;Windows;10;Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3269.3 Safari/537.36;94.25.239.174;$$CUSTOM_PARAM(cd11)$$;$$CUSTOM_PARAM(cd7)$$;$$CUSTOM_PARAM(cd10)$$',	'cd31': '$$CUSTOM_PARAM(cd31)$$',	'pr1ca': 'Smartlink',	'sr': '2560x1080',	'cc': '$$CUSTOM_PARAM(cc)$$',	'a': '390101226',	'cd18': '1511878959396.vkx2qjja',	'cd16': '$$CUSTOM_PARAM(cd16)$$',	'cd17': '$$CUSTOM_PARAM(cd17)$$',	'cm': 'Blind',	'cd14': '$$CUSTOM_PARAM(cd14)$$',	'cn': 'Mondia Media|Smartlink (Adult)_copy',	'cd15': 'Blind',	'cd12': 'Mondia%20Media',	'cd13': '$$CUSTOM_PARAM(cd13)$$',	'cd10': '$$CUSTOM_PARAM(cd10)$$',	'cd11': '2145358953.1511857872',	'cs': 'Mondia Media',	'cd20': '$$CUSTOM_PARAM(cd20)$$',	't': 'event',	'v': '1',	'z': '1379155459',	'de': 'UTF-8',	'dl': 'http://n38adshostnet.com/ads?key=b7e41cd81d999242866d54770553fce6',	'cd49': '$$CUSTOM_PARAM(cd49)$$',	'cd47': '$$CUSTOM_PARAM(cd47)$$',	'cd48': '94.25.239.174',	'cd45': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',	'cd46': 'en-US,en;q=0.9',	'tid': 'UA-98835143-2',	'cd43': '$$CUSTOM_PARAM(cd43)$$',	'cd44': '$$CUSTOM_PARAM(cd44)$$',	'cd52': '57.62987',	'cd53': '$$CUSTOM_PARAM(cd53)$$',	'cd50': 'en',	'cd51': '$$CUSTOM_PARAM(cd51)$$',	'pr1id': '232',	'ul': 'en-us',	'ea': 'Click',	'ec': 'AdClick',	'pr1br': 'Avazu (AD)',	'cd38': '$$CUSTOM_PARAM(cd38)$$',	'cd39': '$$CUSTOM_PARAM(cd39)$$',	'cd36': '$$CUSTOM_PARAM(cd36)$$',	'cd37': '$$CUSTOM_PARAM(cd37)$$',	'cd34': '$$CUSTOM_PARAM(cd34)$$',	'cd35': 'unknown',	'cd32': '7ced3960-d416-11e7-af65-d48564c62f44',	'cd33': 'Russian Federation',	'cd41': '$$CUSTOM_PARAM(cd41)$$',	'cd42': '$$CUSTOM_PARAM(cd42)$$',	'cd40': '$$CUSTOM_PARAM(cd40)$$',	'vp': '2420x960',	'ni': '0',	'cid': '2145358953.1511857872',	'cd69': 'Yaroslavskaya oblast',	'cd68': '$$CUSTOM_PARAM(cd68)$$',	'cd65': '$$CUSTOM_PARAM(cd65)$$',	'cd74': '$$CUSTOM_PARAM(cd74)$$',	'cd75': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3269.3 Safari/537.36',	'cd72': '$$CUSTOM_PARAM(cd72)$$',	'cd73': '1511878958962',	'cd70': '$$CUSTOM_PARAM(cd70)$$',	'cd71': '$$CUSTOM_PARAM(cd71)$$',	'hitId': '1bfd4660-7113-4b49-8fe5-30027d16477c',	'cd58': '10',	'cd59': '$$CUSTOM_PARAM(cd59)$$',	'cd56': 'Windows',	'cd57': 'unknown',	'cd54': '39.87368',	'cd55': '$$CUSTOM_PARAM(cd55)$$',	'cd2': 'Avazu (AD)',	'cd63': 'http',	'cd1': 'Russian Federation;Yaroslavskaya oblast;Yaroslavl;unknown;unknown;Windows;10;Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3269.3 Safari/537.36;94.25.239.174;$$CUSTOM_PARAM(cd11)$$;$$CUSTOM_PARAM(cd7)$$;$$CUSTOM_PARAM(cd10)$$',	'cd64': '$$CUSTOM_PARAM(cd64)$$',	'pa': 'click',	'cd4': '$$CUSTOM_PARAM(cd4)$$',	'cd61': '580',	'cd3': '4039222 - Adult Smartlink Global (Adult)',	'cd62': 'Smartlink (Adult)_copy',	'_s': '1',	'cd6': 'MjEjMjMyIzE2IzU4MHw4Mzl8UlV8M3wxfHx8d2J2MzRwOTI5ajc2fDdjZWQzOTYwLWQ0MTYtMTFlNy1hZjY1LWQ0ODU2NGM2MmY0NHx8',	'cd5': '$$CUSTOM_PARAM(cd5)$$',	'cd60': '$$CUSTOM_PARAM(cd60)$$',	'_u': 'SCCAAEAL~',	'cd8': '$$CUSTOM_PARAM(cd8)$$',	'_v': 'j66',	'cd7': '$$CUSTOM_PARAM(cd7)$$',	'cd9': '$$CUSTOM_PARAM(cd9)$$',	'time': '1511878959416'}}";

	@Test
	public void testTableRowCreator() throws IOException {

		for (int i = 0; i < 100; i++) {
			TableRow tableRow = new TableRowCreator(HIT_JSON).getTableRow();
			Assert.assertEquals(TABLE_ROW_JSON, tableRow.toPrettyString());
			System.out.println(tableRow.toPrettyString());

		}
	}
}
