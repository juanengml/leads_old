CREATE TABLE leads_broker (
	cpf TEXT PRIMARY KEY,
	capacity INTEGER DEFAULT -1 NOT NULL,
	uf TEXT NOT NULL
);
CREATE UNIQUE INDEX leads_broker_cpf_IDX ON leads_broker (cpf);

CREATE TABLE leads_lead (
	cpf TEXT PRIMARY KEY
);
CREATE UNIQUE INDEX leads_lead_cpf_IDX ON leads_lead (cpf);

-- leads_match definition

CREATE TABLE leads_match (
	id_match INTEGER PRIMARY KEY ,
	"date" TEXT NOT NULL,
	score REAL NOT NULL,
	cpf_broker TEXT NOT NULL,
	cpf_lead TEXT NOT NULL,
	method TEXT NOT NULL,
	CONSTRAINT leads_match_leads_broker_FK FOREIGN KEY (cpf_broker) REFERENCES leads_broker(cpf_broker) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT leads_match_leads_lead_FK FOREIGN KEY (cpf_lead) REFERENCES leads_lead(cpf_lead) ON DELETE RESTRICT ON UPDATE RESTRICT
);

