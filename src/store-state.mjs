import { CardError, assert } from './errors.mjs';
import { emptyState, checkState } from './model.mjs';

export function decode(raw) {
  try { return checkState(raw ? JSON.parse(raw) : emptyState()); }
  catch (e) { if (e instanceof CardError) throw e;
    throw new CardError('STORE_CORRUPT', 'Cannot parse the registry. Preserve the file/key for recovery.', 503); }
}
export function encode(state) {
  checkState(state);
  const raw = JSON.stringify(state);
  assert(Buffer.byteLength(raw) <= 3_000_000, 'STORE_FULL', 'Registry exceeds its V1 bound; archive policy needs review.', 503);
  return raw;
}
