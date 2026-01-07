import { sleep } from 'k6';
import tracing from 'k6/x/tracing';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

export const options = {
  vus: 1,
  duration: '24h',
};

const endpoint = __ENV.ENDPOINT || 'tempo:4317';
const orgid = __ENV.TEMPO_X_SCOPE_ORGID || 'k6-test';

const client = new tracing.Client({
  endpoint,
  exporter: tracing.EXPORTER_OTLP,
  tls: {
    insecure: true,
  },
  headers: {
    'X-Scope-Orgid': orgid,
  },
});

const traceDefaults = {
  attributeSemantics: tracing.SEMANTICS_HTTP,
  randomAttributes: { count: 2, cardinality: 5 },
  randomEvents: { count: 0.1 },
};

// Template 1: Cross-attribute matching (someattr1 == someattr2)
// This creates traces where we can test: WHERE span_attributes['someattr1'] = span_attributes['someattr2']
function createMatchingAttrsTemplate() {
  const matchingValue = String(randomIntBetween(0, 9));
  return {
    defaults: {
      ...traceDefaults,
      attributes: {
        someattr1: matchingValue,
        someattr2: matchingValue, // Same value for cross-attribute testing
      },
    },
    spans: [
      {
        service: 'test-service',
        name: 'list-articles',
        duration: { min: 100, max: 500 },
        attributes: {
          'http.request.method': 'GET',
          'http.response.status_code': 200,
          'net.host.port': 8604,
        },
      },
      {
        service: 'backend-service',
        name: 'delete-checkout',
        duration: { min: 50, max: 200 },
        attributes: {
          someattr1: matchingValue,
          someattr2: matchingValue,
        },
      },
    ],
  };
}

// Template 2: Random attributes (someattr1 != someattr2 usually)
function createRandomAttrsTemplate() {
  return {
    defaults: {
      ...traceDefaults,
      attributes: {
        someattr1: String(randomIntBetween(0, 9)),
        someattr2: String(randomIntBetween(0, 9)),
      },
    },
    spans: [
      {
        service: 'api-service',
        name: ['list-articles', 'delete-checkout', 'get-user', 'create-order'][randomIntBetween(0, 3)],
        duration: { min: 200, max: 800 },
        attributes: {
          'http.request.method': ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'][randomIntBetween(0, 4)],
          'http.response.status_code': [200, 201, 400, 404, 500][randomIntBetween(0, 4)],
          'net.host.port': [8604, 8080, 8443, 9000][randomIntBetween(0, 3)],
          someattr1: String(randomIntBetween(0, 9)),
          someattr2: String(randomIntBetween(0, 9)),
        },
      },
      {
        service: 'data-service',
        name: ['list-articles', 'delete-checkout'][randomIntBetween(0, 1)],
        duration: { min: 50, max: 150 },
        attributes: {
          someattr1: String(randomIntBetween(0, 9)),
          someattr2: String(randomIntBetween(0, 9)),
        },
      },
    ],
  };
}

// Template 3: PATCH with 200 status code
// This creates traces for testing: WHERE http.request.method = 'PATCH' AND http.response.status_code = 200
function createPatchWith200Template() {
  return {
    defaults: {
      ...traceDefaults,
      attributes: {
        someattr1: String(randomIntBetween(0, 9)),
        someattr2: String(randomIntBetween(0, 9)),
      },
    },
    spans: [
      {
        service: 'api-service',
        name: ['list-articles', 'update-profile'][randomIntBetween(0, 1)],
        duration: { min: 300, max: 1000 },
        attributes: {
          'http.request.method': 'PATCH',
          'http.response.status_code': 200,
          'net.host.port': [8604, 8080][randomIntBetween(0, 1)],
          someattr1: String(randomIntBetween(0, 9)),
          someattr2: String(randomIntBetween(0, 9)),
        },
      },
      {
        service: 'backend-service',
        name: 'delete-checkout',
        duration: { min: 100, max: 300 },
        attributes: {
          'http.request.method': 'PATCH',
          'http.response.status_code': 200,
        },
      },
    ],
  };
}

// Template 4: Port 8604
// This creates traces for testing: WHERE net.host.port = 8604
function createPort8604Template() {
  return {
    defaults: {
      ...traceDefaults,
      attributes: {
        someattr1: String(randomIntBetween(0, 9)),
        someattr2: String(randomIntBetween(0, 9)),
      },
    },
    spans: [
      {
        service: 'port-service',
        name: ['list-articles', 'delete-checkout', 'get-user'][randomIntBetween(0, 2)],
        duration: { min: 150, max: 600 },
        attributes: {
          'net.host.port': 8604,
          'http.request.method': ['GET', 'POST', 'PUT'][randomIntBetween(0, 2)],
          'http.response.status_code': [200, 400, 500][randomIntBetween(0, 2)],
          someattr1: String(randomIntBetween(0, 9)),
          someattr2: String(randomIntBetween(0, 9)),
        },
      },
      {
        service: 'worker-service',
        name: 'list-articles',
        duration: { min: 50, max: 200 },
        attributes: {
          'net.host.port': 8604,
          someattr1: String(randomIntBetween(0, 9)),
          someattr2: String(randomIntBetween(0, 9)),
        },
      },
    ],
  };
}

// Template 5: Specific span names
// This creates traces for testing: WHERE span_name = 'list-articles' OR span_name = 'delete-checkout'
function createTargetSpanNamesTemplate() {
  return {
    defaults: {
      ...traceDefaults,
      attributes: {
        someattr1: String(randomIntBetween(0, 9)),
        someattr2: String(randomIntBetween(0, 9)),
      },
    },
    spans: [
      {
        service: 'shop-backend',
        name: 'list-articles',
        duration: { min: 200, max: 700 },
        attributes: {
          'http.request.method': 'GET',
          'http.response.status_code': 200,
          someattr1: String(randomIntBetween(0, 9)),
          someattr2: String(randomIntBetween(0, 9)),
        },
      },
      {
        service: 'cart-service',
        name: 'delete-checkout',
        duration: { min: 100, max: 400 },
        attributes: {
          'http.request.method': 'DELETE',
          'http.response.status_code': 204,
          someattr1: String(randomIntBetween(0, 9)),
          someattr2: String(randomIntBetween(0, 9)),
        },
      },
    ],
  };
}

// All template generators with weights
const templateGenerators = [
  { weight: 25, generator: createMatchingAttrsTemplate },  // Cross-attribute matching
  { weight: 25, generator: createRandomAttrsTemplate },     // Random attrs
  { weight: 15, generator: createPatchWith200Template },    // PATCH + 200
  { weight: 15, generator: createPort8604Template },        // Port 8604
  { weight: 20, generator: createTargetSpanNamesTemplate }, // list-articles / delete-checkout
];

// Select a template based on weights
function selectTemplate() {
  const totalWeight = templateGenerators.reduce((sum, t) => sum + t.weight, 0);
  let random = randomIntBetween(1, totalWeight);
  
  for (const t of templateGenerators) {
    random -= t.weight;
    if (random <= 0) {
      return t.generator();
    }
  }
  return templateGenerators[0].generator();
}

export default function () {
  const template = selectTemplate();
  const gen = new tracing.TemplatedGenerator(template);
  client.push(gen.traces());

  sleep(randomIntBetween(1, 3));
}

export function teardown() {
  client.shutdown();
}
