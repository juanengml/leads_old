CREATE TABLE leads_broker (
	id_leads_broker INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	cpf TEXT NOT NULL,
	capacity INTEGER DEFAULT -1 NOT NULL,
	fair_indice INTEGER DEFAULT -1 NOT NULL
);
CREATE UNIQUE INDEX leads_broker_cpf_IDX ON leads_broker (cpf);

CREATE TABLE leads_lead (
	id_leads_lead INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	cpf TEXT NOT NULL
);
CREATE UNIQUE INDEX leads_lead_cpf_IDX ON leads_lead (cpf);

-- leads_match definition

CREATE TABLE leads_match (
	"data" TEXT NOT NULL,
	score REAL NOT NULL,
	id_leads_broker INTEGER NOT NULL,
	id_leads_lead INTEGER NOT NULL,
	CONSTRAINT leads_match_leads_broker_FK FOREIGN KEY (id_leads_broker) REFERENCES leads_broker(id_leads_broker) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT leads_match_leads_lead_FK FOREIGN KEY (id_leads_lead) REFERENCES leads_lead(id_leads_lead) ON DELETE RESTRICT ON UPDATE RESTRICT
);

