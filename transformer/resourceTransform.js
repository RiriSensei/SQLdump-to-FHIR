const fs = require('fs');
const fsPromises = require('fs').promises;
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const { open } = require('sqlite');
const winston = require('winston');
const { Transform } = require('stream');
const JSONStream = require('JSONStream');

class FHIRTransformer {
    constructor(config) {
        // Configurable parameters
        this.config = {
            inputDir: config.inputDir || 'intermediate',
            outputPath: config.outputPath || 'output/fhir_resources.sqlite',
            batchSize: config.batchSize || 10000,
            logLevel: config.logLevel || 'info'
        };

        // Setup logging
        this.logger = this.setupLogger();

        // db connection
        this.db = null;

        // Resource mappings
        this.resourceMappings = {
            'tb_person_mtr_preprocessed': {
                tableName: 'Patient',
                mapper: this.mapPatient.bind(this),
                requiredFields: ['person_id', 'person_fname'],
                primaryKey: 'person_id'
            },
            'tb_encounter_preprocessed': {
                tableName: 'Encounter',
                mapper: this.mapEncounter.bind(this),
                requiredFields: ['enc_id', 'enc_date'],
                primaryKey: 'enc_id'
            },
            'tb_emr_surgery_info_preprocessed': {
                tableName: 'Procedure',
                mapper: this.mapProcedure.bind(this),
                requiredFields: ['entry_id'],
                primaryKey: 'entry_id'
            },
            'tb_mig_implant_description_preprocessed': {
                tableName: 'Device',
                mapper: this.mapDevice.bind(this),
                requiredFields: ['entry_id'],
                primaryKey: 'entry_id'
            }
        };
    }

    // Utility Methods for Data Cleaning and Validation
    cleanString(value) {
        // Convert numeric 0 or 1 to empty string, trim, and validate
        if (value === 0 || value === 1 || value === '0') return '';
        return typeof value === 'string' ? value.trim() : value;
    }

    isValidString(value) {
        return value && value !== '0' && value !== 0;
    }

    formatDate(dateInput) {
        if (!dateInput || dateInput === 0) return null;
        try {
            const date = new Date(dateInput);
            return isNaN(date) ? null : date.toISOString().split('T')[0];
        } catch {
            return null;
        }
    }

    formatDateTime(dateTimeInput) {
        if (!dateTimeInput || dateTimeInput === 0) return null;
        try {
            const dateTime = new Date(dateTimeInput);
            return isNaN(dateTime) ? null : dateTime.toISOString();
        } catch {
            return null;
        }
    }

    // Mapping Methods
    mapPatient(row) {
        if (!row) {
            this.logger.warn('Received undefined row in mapPatient');
            return null;
        }

        try {
            //gender and title mapping
            const genderMap = {
                1: 'male',
                2: 'female',
                3: 'other'
            };

            const titleMap = {
                1: 'Mr.',
                2: 'Ms.',
                3: 'Mrs.'
            };

            const patient = {
                resourceType: 'Patient',
                id: `patient-${row.person_id}`,
                identifier: [
                    {
                        type: { text: 'Internal ID' },
                        value: row.person_id.toString()
                    },
                    ...(this.isValidString(row.person_uid_old) ? [{
                        type: { text: 'Old Unique Identifier' },
                        value: this.cleanString(row.person_uid_old)
                    }] : [])
                ],
                active: row.person_active === 1 || row.person_active === true,
                name: [{
                    use: 'official',
                    family: this.cleanString(row.person_fname) || 'Unknown',
                    given: this.isValidString(row.person_mname) ? [this.cleanString(row.person_mname)] : [],
                    prefix: this.isValidString(row.person_title) 
                        ? [titleMap[row.person_title] || ''] 
                        : []
                }],
                gender: genderMap[row.person_sex] || 'unknown',
                birthDate: this.formatDate(row.person_dob || row.approx_dob),
                extension: [
                    ...(this.isValidString(row.occupation) ? [{
                        valueString: this.cleanString(row.occupation)
                    }] : []),
                    {
                        valueDateTime: this.formatDateTime(row.person_datereg)
                    },
                    ...(this.isValidString(row.nationality) ? [{
                        valueString: this.cleanString(row.nationality)
                    }] : [])
                ]
            };

            return patient;
        } catch (error) {
            this.logger.error('Error in mapPatient', { 
                error: error.message, 
                rowData: JSON.stringify(row).slice(0, 500) 
            });
            return null;
        }
    }

    mapEncounter(row) {
        if (!row) {
            this.logger.warn('Received undefined row in mapEncounter');
            return null;
        }

        try {
            //status and visit type mapping
            const statusMap = {
                1: 'finished',
                2: 'in-progress',
                3: 'planned'
            };

            const visitTypeMap = {
                1: 'outpatient',
                2: 'inpatient',
                3: 'emergency'
            };

            const encounter = {
                resourceType: 'Encounter',
                id: `encounter-${row.enc_id}`,
                status: statusMap[row.enc_status] || 'unknown',
                class: {
                    system: 'http://terminology.hl7.org/CodeSystem/v3-ActCode',
                    code: visitTypeMap[row.enc_typeofvisit] || 'AMB',
                    display: visitTypeMap[row.enc_typeofvisit] || 'Ambulatory'
                },
                subject: row.enc_patid ? { 
                    reference: `Patient/patient-${row.enc_patid}` 
                } : null,
                period: {
                    start: this.formatDateTime(row.enc_date),
                    end: this.formatDateTime(row.enc_timeofconv)
                },
                reasonCode: this.isValidString(row.enc_reason) ? [{
                    text: this.cleanString(row.enc_reason)
                }] : [],
                extension: [
                    ...(row.enc_delay_min ? [{
                        valueInteger: parseInt(row.enc_delay_min)
                    }] : []),
                    {
                        valueInteger: row.org_id
                    },
                    ...(this.isValidString(row.enc_department) ? [{
                        valueString: this.cleanString(row.enc_department)
                    }] : [])
                ]
            };

            return encounter;
        } catch (error) {
            this.logger.error('Error in mapEncounter', { 
                error: error.message, 
                rowData: JSON.stringify(row).slice(0, 500) 
            });
            return null;
        }
    }

    mapProcedure(row) {
        if (!row) {
            this.logger.warn('Received undefined row in mapProcedure');
            return null;
        }

        try {
            const procedure = {
                resourceType: 'Procedure',
                id: `procedure-${row.entry_id}`,
                status: 'completed',
                code: {
                    coding: [{
                        display: this.isValidString(row.procedure_name) 
                            ? this.cleanString(row.procedure_name) 
                            : 'Unspecified Procedure'
                    }]
                },
                subject: row.encounter_id ? { 
                    reference: `Patient/patient-${row.encounter_id}` 
                } : null,
                performedDateTime: this.formatDateTime(row.entry_date),
                extension: [
                    ...(this.isValidString(row.procedure_site) ? [{
                        valueString: this.cleanString(row.procedure_site)
                    }] : []),
                    ...(this.isValidString(row.anesthesia_type) ? [{
                        valueString: this.cleanString(row.anesthesia_type)
                    }] : []),
                    ...(row.procedure_duration ? [{
                        valueInteger: parseInt(row.procedure_duration)
                    }] : [])
                ]
            };

            return procedure;
        } catch (error) {
            this.logger.error('Error in mapProcedure', { 
                error: error.message, 
                rowData: JSON.stringify(row).slice(0, 500) 
            });
            return null;
        }
    }

    mapDevice(row) {
        if (!row) {
            this.logger.warn('Received undefined row in mapDevice');
            return null;
        }

        try {
            const device = {
                resourceType: 'Device',
                id: `device-${row.entry_id}`,
                type: {
                    coding: [{
                        display: this.isValidString(row.ImplantName) 
                            ? this.cleanString(row.ImplantName) 
                            : 'Unknown Device'
                    }]
                },
                patient: row.encounter_id ? { 
                    reference: `Patient/patient-${row.encounter_id}` 
                } : null,
                lotNumber: this.isValidString(row.Specification) 
                    ? this.cleanString(row.Specification) 
                    : null,
                extension: [
                    {
                        valueDateTime: this.formatDateTime(row.entry_date)
                    },
                    ...(this.isValidString(row.manufacturer) ? [{
                        valueString: this.cleanString(row.manufacturer)
                    }] : [])
                ]
            };

            return device;
        } catch (error) {
            this.logger.error('Error in mapDevice', { 
                error: error.message, 
                rowData: JSON.stringify(row).slice(0, 500) 
            });
            return null;
        }
    }// remember to copy the rest fro previous implementation

    //tasks for tmrw: 1.clean code, 2.generate exception/error handling for all functions from ai








    


    // Setup Logger Method
    setupLogger() {
        return winston.createLogger({
            level: this.config.logLevel,
            format: winston.format.combine(
                winston.format.timestamp({
                    format: 'YYYY-MM-DD HH:mm:ss'
                }),
                winston.format.errors({ stack: true }),
                winston.format.splat(),
                winston.format.json()
            ),
            transports: [
                new winston.transports.Console({
                    format: winston.format.combine(
                        winston.format.colorize(),
                        winston.format.simple()
                    )
                }),
                new winston.transports.File({ 
                    filename: 'logs/transformation-error.log', 
                    level: 'error',
                    maxsize: 5 * 1024 * 1024, // 5MB
                    maxFiles: 5
                }),
                new winston.transports.File({ 
                    filename: 'logs/transformation.log',
                    maxsize: 10 * 1024 * 1024, // 10MB
                    maxFiles: 10
                })
            ]
        });
    }

    async initializeDatabase() {
        try {
            // Ensure log and output directories exist using fsPromises
            await fsPromises.mkdir(path.dirname(this.config.outputPath), { recursive: true });
            await fsPromises.mkdir('logs', { recursive: true });
    
            // Open database with better connection management
            this.db = await open({
                filename: this.config.outputPath,
                driver: sqlite3.Database
            });
    
            // Performance and durability settings
            await this.db.exec(`
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                PRAGMA cache_size=-10000;
                PRAGMA busy_timeout=5000;
            `);
    
            // Create tables with advanced indexing
            await this.createTables();
    
            this.logger.info('Database initialized successfully');
        } catch (error) {
            this.logger.error('Database initialization failed', { error: error.message });
            throw error;
        }
    }

    // Comprehensive table creation with indexing
    async createTables() {
        const tableDefinitions = [
            {
                name: 'Patient',
                columns: [
                    'id TEXT PRIMARY KEY',
                    'resource TEXT',
                    'created_at DATETIME DEFAULT CURRENT_TIMESTAMP'
                ],
                indexes: [
                    'CREATE INDEX IF NOT EXISTS idx_patient_created ON Patient(created_at)',
                    'CREATE INDEX IF NOT EXISTS idx_patient_resource ON Patient(id)'
                ]
            },
            {
                name: 'Encounter',
                columns: [
                    'id TEXT PRIMARY KEY',
                    'resource TEXT',
                    'created_at DATETIME DEFAULT CURRENT_TIMESTAMP'
                ],
                indexes: [
                    'CREATE INDEX IF NOT EXISTS idx_encounter_created ON Encounter(created_at)',
                    'CREATE INDEX IF NOT EXISTS idx_encounter_resource ON Encounter(id)'
                ]
            },
            {
                name: 'Procedure',
                columns: [
                    'id TEXT PRIMARY KEY',
                    'resource TEXT',
                    'created_at DATETIME DEFAULT CURRENT_TIMESTAMP'
                ],
                indexes: [
                    'CREATE INDEX IF NOT EXISTS idx_procedure_created ON Procedure(created_at)',
                    'CREATE INDEX IF NOT EXISTS idx_procedure_resource ON Procedure(id)'
                ]
            },
            {
                name: 'Device',
                columns: [
                    'id TEXT PRIMARY KEY',
                    'resource TEXT',
                    'created_at DATETIME DEFAULT CURRENT_TIMESTAMP'
                ],
                indexes: [
                    'CREATE INDEX IF NOT EXISTS idx_device_created ON Device(created_at)',
                    'CREATE INDEX IF NOT EXISTS idx_device_resource ON Device(id)'
                ]
            }
        ];

        for (const table of tableDefinitions) {
            await this.db.exec(`
                CREATE TABLE IF NOT EXISTS ${table.name} (
                    ${table.columns.join(',')}
                )
            `);

            // Create indexes
            for (const indexSql of table.indexes) {
                await this.db.exec(indexSql);
            }
        }
    }

    // Stream-based resource transformation
    async transformResourcesStreaming() {
        const start = Date.now();
        this.logger.info('Starting streaming transformation');

        try {
            const files = await fsPromises.readdir(this.config.inputDir);

            for (const file of files) {
                const filePath = path.join(this.config.inputDir, file);
                const tableName = path.basename(file, '.json');

                if (this.resourceMappings[tableName]) {
                    const mapping = this.resourceMappings[tableName];
                    
                    this.logger.info(`Processing file: ${file}`);

                    // Use streams for memory-efficient processing
                    await new Promise((resolve, reject) => {
                        const readStream = fs.createReadStream(filePath, { encoding: 'utf8' });
                        const parser = JSONStream.parse('*');
                        
                        const transformStream = new Transform({
                            objectMode: true,
                            transform: async (row, encoding, callback) => {
                                try {
                                    const mappedResource = mapping.mapper(row);
                                    if (mappedResource) {
                                        await this.saveResource(mappedResource, mapping.tableName);
                                    }
                                    callback();
                                } catch (error) {
                                    this.logger.error('Transformation error', { 
                                        file, 
                                        error: error.message,
                                        row: JSON.stringify(row).slice(0, 200) 
                                    });
                                    callback(error);
                                }
                            }
                        });

                        readStream
                            .pipe(parser)
                            .pipe(transformStream)
                            .on('finish', resolve)
                            .on('error', reject);
                    });
                }
            }

            const duration = (Date.now() - start) / 1000;
            this.logger.info(`Transformation completed in ${duration} seconds`);
        } catch (error) {
            this.logger.error('Transformation failed', { error: error.message });
            throw error;
        }
    }

    // Optimized resource saving
    async saveResource(resource, tableName) {
        try {
            const stmt = await this.db.prepare(`
                INSERT OR REPLACE INTO ${tableName} (id, resource) 
                VALUES (?, ?)
            `);
            
            await stmt.run(resource.id, JSON.stringify(resource));
            await stmt.finalize();
        } catch (error) {
            this.logger.error('Resource saving error', { 
                tableName, 
                resourceId: resource.id,
                error: error.message 
            });
            throw error;
        }
    }


    // Clean shutdown
    async close() {
        if (this.db) {
            this.logger.info('Closing database connection');
            await this.db.close();
        }
    }
}

// Main execution
async function main() {
    const transformer = new FHIRTransformer({
        inputDir: 'intermediate',
        outputPath: 'output/fhir_resources.sqlite',
        batchSize: 10000,
        logLevel: 'info'
    });

    try {
        await transformer.initializeDatabase();
        await transformer.transformResourcesStreaming();
    } catch (error) {
        console.error('Transformation process failed:', error);
    } finally {
        await transformer.close();
    }
}

// Error handling for unhandled promises
process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

main().catch(console.error);