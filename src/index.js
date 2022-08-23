import db from "./db.js"
import {
  easyMint,
  routerCanister,
  pwrCanister,
  getMap,
  AccountIdentifier,
  PrincipalFromSlot,
  nftCanister,
  historyCanister,
  anvilCanister,
} from "@vvv-interactive/nftanvil";

import {
  encodeTokenId,
  decodeTokenId,
  tokenUrl,
  ipfsTokenUrl,
  tokenToText,
  tokenFromText,
} from "@vvv-interactive/nftanvil-tools/cjs/token.js";

import JSONbig from "json-bigint";

let { principal, address, subaccount } = await routerCanister();

const loop = async () => {
let map = await getMap();

let slot = map.history; // last slot

let historyCan = PrincipalFromSlot(map.space, slot);

let history = historyCanister(historyCan);

let info = await history.info();
let { total } = info;


let cur_max = await db.query("SELECT MAX(idx) as mx FROM transaction WHERE slot = ?",[slot]);
cur_max = cur_max[0]['mx']+1;
if (!cur_max) cur_max = 0;

for (let i=cur_max; i<total; i+=100) {
	let x = await history.list({from:i, to:i+100});
	for (let [txidx, tx] of x.entries()) {
		if (!tx[0]) continue;
		let gidx = i + txidx;
		let info = tx[0].info;
		let obj = Object.keys(info)[0];
		let action = Object.keys(info[obj])[0];
		let data = info[obj][action];
		let rez = await db.query("INSERT INTO `transaction` SET ?",{data: JSONbig.stringify(data), action, obj, idx:gidx, slot});
	}
}


//await db.query("TRUNCATE TABLE nft");

let nft_digest = (await db.query("SELECT val from conf WHERE name = 'nft_digest'"))[0]['val'];
let purchase_digest = (await db.query("SELECT val from conf WHERE name = 'purchase_digest'"))[0]['val'];
let tx_total = (await db.query("SELECT count(*) as cnt FROM transaction"))[0]['cnt'];


for (let i = purchase_digest+1; i < tx_total; i++) {
	let {data,slot,idx,obj,action} = (await db.query("SELECT * FROM transaction WHERE 1 ORDER BY id ASC LIMIT "+i+",1"))[0];
	let d = JSON.parse(data);

	let created = Math.floor(d.created / 1000000000);

	if (obj == "nft" && action == "purchase") {

		let x = {
		tid : d.token,
		created: { toSqlString: function() { return 'FROM_UNIXTIME('+created+')'; } },
		seller: AccountIdentifier.ArrayToText(d.seller),
		buyer: AccountIdentifier.ArrayToText(d.buyer),
		amount: d.amount,
		recharge: d.recharge,
		author_address: AccountIdentifier.ArrayToText(d.author.address),
		author_share: d.author.share,
		marketplace_address: d.marketplace[0]?AccountIdentifier.ArrayToText(d.marketplace[0].address):null,
		marketplace_share: d.marketplace[0]?d.marketplace[0].share:null,
		affiliate_address: d.affiliate[0]?AccountIdentifier.ArrayToText(d.affiliate[0].address):null,
		affiliate_share: d.affiliate[0]?d.affiliate[0].share:null,
		};

		x.author_profit = x.author_share ? (x.amount * x.author_share/10000) : 0;
		x.marketplace_profit = x.marketplace_share ? (x.amount * x.marketplace_share/10000) : 0;
		x.affiliate_profit = x.affiliate_share ? (x.amount * x.affiliate_share/10000) : 0;
		x.anvil_profit = x.amount * 50 / 10000;
		x.seller_profit = x.amount - x.anvil_profit - x.affiliate_profit - x.marketplace_profit - x.author_profit;

		await db.query("INSERT INTO purchase SET ?", x);
	}

}
await db.query("UPDATE conf SET val = ? WHERE name = 'purchase_digest' ", [tx_total -1]);


for (let i = nft_digest+1; i < tx_total; i++) {
	let {data,slot,idx,obj,action} = (await db.query("SELECT * FROM transaction WHERE 1 ORDER BY id ASC LIMIT "+i+",1"))[0];
	let d = JSON.parse(data);

	let created = Math.floor(d.created / 1000000000);

	if (obj == "nft" && action == "mint") {
		let user = AccountIdentifier.ArrayToText(d.user);
		let tid = d.token;
		await db.query("INSERT INTO nft SET ?", {id: tid, author: user, bearer: user, created: { toSqlString: function() { return 'FROM_UNIXTIME('+created+')'; } }});
	}
	
	if (obj == "nft" && action == "burn") {
		let tid = d.token;	
		await db.query("DELETE FROM nft WHERE id = ?",[tid]);
	};
	if (obj == "nft" && action == "price") {
		let tid = d.token;	
		let user = AccountIdentifier.ArrayToText(d.user);
		let price = d.price.amount;
		await db.query("UPDATE nft SET price = ? WHERE id = ?",[price, tid]);

	};
	if (obj == "nft" && action == "transfer") {
		let tid = d.token;	
		let from = AccountIdentifier.ArrayToText(d.from);
		let to = AccountIdentifier.ArrayToText(d.to);
		await db.query("UPDATE nft SET bearer = ?, price = 0 WHERE id = ?",[to, tid]);

	};
	if (obj == "nft" && action == "purchase") {
		let tid = d.token;	
		let bearer = AccountIdentifier.ArrayToText(d.buyer);
		await db.query("UPDATE nft SET bearer = ?, price = 0 WHERE id = ?",[bearer, tid]);
	};
	if (obj == "nft" && action == "socket") {
		let tid = d.plug;	
		await db.query("UPDATE nft SET bearer = NULL, price =0 WHERE id = ?",[tid]);
	};	
	if (obj == "nft" && action == "unsocket") {
		let tid = d.plug;	
		let user = AccountIdentifier.ArrayToText(d.user);
		await db.query("UPDATE nft SET bearer = ?, price =0 WHERE id = ?",[user, tid]);
	};

}; 

await db.query("UPDATE conf SET val = ? WHERE name = 'nft_digest' ", [tx_total -1]);
};

const delay = (ms) => new Promise((resolve,reject) => { setTimeout(resolve, ms) })

const fetchMeta = async () => {
	while(true) {
		let rx = (await db.query("SELECT id FROM nft WHERE meta is NULL"));
		if (!rx || !rx[0]) return;
		let tid = rx[0]['id'];

		try {
		let { slot } = decodeTokenId(tid);


		let map = await getMap();
		let canister = PrincipalFromSlot(map.space, slot).toText();

		let nftcan = nftCanister(canister);
		let res = await nftcan.metadata(tid);
		let m = res.ok.data;
		let fin = {
			quality: m.quality,
			lore: m.lore[0],
			name: m.name[0],
			tags : m.tags,
			secret: m.secret,
			attributes: m.attributes,
	//		custom: m.custom[0]
		};
		await db.query("UPDATE nft SET meta = ? WHERE id = ?",[JSONbig.stringify(fin), tid]);

		} catch (e) {
			await db.query("UPDATE nft SET meta = ? WHERE id = ?",["{}", tid]);
			console.log(e);
		}
	}
}


while (true) {
	try {
	await fetchMeta();
	await loop();
	} catch (e) {console.error(e);}
	console.log(".");
	await delay(3000);
};


