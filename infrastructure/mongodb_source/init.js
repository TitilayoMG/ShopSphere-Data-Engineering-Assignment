const dbName = 'shopsphere_events';
db = db.getSiblingDB(dbName);
db.customer_sessions.drop();
db.product_reviews.drop();

const cities = [
  { city: 'Lagos', state: 'Lagos', country: 'Nigeria' },
  { city: 'Abuja', state: 'FCT', country: 'Nigeria' },
  { city: 'Port Harcourt', state: 'Rivers', country: 'Nigeria' },
  { city: 'Ibadan', state: 'Oyo', country: 'Nigeria' },
  { city: 'Kano', state: 'Kano', country: 'Nigeria' },
  { city: 'Enugu', state: 'Enugu', country: 'Nigeria' }
];
const devices = [{ type: 'mobile', os: 'Android' }, { type: 'desktop', os: 'Windows' }, { type: 'mobile', os: 'iOS' }, { type: 'tablet', os: 'Android' }];
const browsers = ['Chrome', 'Safari', 'Edge', 'Firefox'];
const eventTypes = ['product_view', 'search', 'add_to_cart', 'remove_from_cart', 'checkout_started', 'purchase_completed'];
const searchTerms = ['phone', 'laptop', 'sneakers', 'rice cooker', 'headphones'];

const sessions = [];
for (let i = 1; i <= 200; i += 1) {
  const started = new Date(Date.UTC(2025, 2, 1, 8, 0, 0));
  started.setHours(started.getHours() + i * 3);
  const ended = new Date(started.getTime() + (20 + (i % 45)) * 60000);
  const events = [];
  for (let e = 1; e <= 6; e += 1) {
    const eventTime = new Date(started.getTime() + e * 4 * 60000);
    const type = eventTypes[(i + e) % eventTypes.length];
    const productId = (i % 37 === 0 && e === 2) ? 9999 : 1 + ((i * 5 + e) % 60);
    const event = {
      event_type: type,
      event_time: (i === 44 && e === 3) ? '2025/04/05 10:30:00' : eventTime.toISOString(),
      product_id: ['product_view', 'add_to_cart', 'remove_from_cart', 'purchase_completed'].includes(type) ? productId : null,
      search_term: type === 'search' ? searchTerms[(i + e) % searchTerms.length] : null,
      quantity: ['add_to_cart', 'remove_from_cart', 'purchase_completed'].includes(type) ? 1 + ((i + e) % 3) : null,
      page_url: `/products/${productId}`
    };
    if (i === 25 && (e === 2 || e === 3)) event.event_time = events[0].event_time;
    events.push(event);
  }
  const doc = {
    session_id: `sess_${String(i).padStart(4, '0')}`,
    customer_id: (i === 9 || i === 133) ? null : 1 + ((i * 7) % 120),
    started_at: (i === 78) ? '04-15-2025 12:00:00' : started.toISOString(),
    ended_at: ended.toISOString(),
    browser: browsers[i % browsers.length],
    location: cities[i % cities.length],
    events
  };
  doc.device = (i === 52 || i === 141) ? null : devices[i % devices.length];
  sessions.push(doc);
}
db.customer_sessions.insertMany(sessions);

const reviews = [];
for (let i = 1; i <= 150; i += 1) {
  reviews.push({
    review_id: `rev_${String(i).padStart(4, '0')}`,
    product_id: (i === 88) ? 8888 : 1 + ((i * 3) % 60),
    customer_id: 1 + ((i * 11) % 120),
    rating: (i === 17) ? 6 : ((i === 109) ? 0 : 1 + (i % 5)),
    title: `Review ${i}`,
    review_text: (i === 34 || i === 122) ? null : `Customer review ${i} for ShopSphere product quality and delivery experience.`,
    verified_purchase: i % 4 !== 0,
    created_at: (i === 66) ? '2025.05.06 14:30:00' : new Date(Date.UTC(2025, 3, 1 + (i % 60), 10, 0, 0)).toISOString(),
    helpful_votes: (i * 2) % 31
  });
}
db.product_reviews.insertMany(reviews);

db.customer_sessions.createIndex({ session_id: 1 }, { unique: true });
db.customer_sessions.createIndex({ customer_id: 1 });
db.customer_sessions.createIndex({ started_at: 1 });
db.product_reviews.createIndex({ review_id: 1 }, { unique: true });
db.product_reviews.createIndex({ product_id: 1 });
db.product_reviews.createIndex({ created_at: 1 });
