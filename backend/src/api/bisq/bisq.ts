import config from '../../config';
import * as http from 'http';
import { BisqBlock, BisqTransaction, BisqStats } from './interfaces';
import bisqMarket from './markets-api';
import pricesUpdater from '../../tasks/price-updater';
import logger from '../../logger';

class Bisq {
  private stats: BisqStats = ({
    minted: 0,
    burnt: 0,
    addresses: 0,
    unspent_txos: 0,
    spent_txos: 0,
    height: 0,
    genesisHeight: 0,
    _bsqPrice: 0,
    _usdPrice: 0,
    _marketCap: 0
  });
  private blocks: BisqBlock[] = [];
  private allBlocks: BisqBlock[] = [];
  private blockIndex: { [hash: string]: BisqBlock } = {};
  private lastPollTimestamp: number = 0;
  private pendingQueries: Promise<string>[] = [];

  constructor() { }

  setPriceCallbackFunction(fn: (price: number) => void) {
    bisqMarket.setPriceCallbackFunction(fn);
  }

  public startBisqService(): void {
    logger.info("starting bisq service");

    this.$pollForNewData();
  }

  public async $getTransaction(txId: string): Promise<BisqTransaction | undefined> {
    logger.debug(`getTransaction called from frontend; txId=[${txId}]`);

    if (!this.isBisqAvailable()) return undefined;

    var queriedTx = await this.$lookupBsqTx(txId);
    if (queriedTx !== undefined) {
      this.$fillMissingBlocksInCache(queriedTx.blockHeight, 1);
    }
    return queriedTx;
  }

  public async $getTransactions(start: number, length: number, types: string[]): Promise<[BisqTransaction[], number]> {
    logger.debug(`getTransactions called from frontend; start=[${start}], length=[${length}], types=[${types}]`);

    if (!this.isBisqAvailable()) return [[], 0];

    var transactions = await this.$lookupBsqTxs(0, 2_147_483_647, types);
    return [transactions.slice(start, length + start), transactions.length];
  }

  public async $getBlock(hash: string): Promise<BisqBlock | undefined> {
    logger.debug(`getBlock called from frontend; hash=[${hash}]`);

    var cachedBlock = this.blockIndex[hash];
    if (cachedBlock) {
      return cachedBlock;
    }

    if (!this.isBisqAvailable()) return undefined;

    var queriedBlock = await this.$lookupBsqBlockByHash(hash);
    return queriedBlock;
  }

  public async $getAddress(hash: string): Promise<BisqTransaction[]> {
    logger.debug(`getAddress called from frontend; hash=[${hash}]`);

    if (!this.isBisqAvailable()) return [];

    var queriedTx: BisqTransaction[] = await this.$lookupBsqTxForAddr(hash);
    return queriedTx;
  }

  public getLatestBlockHeight(): number {
    logger.debug(`getLatestBlockHeight called from frontend`);

    return this.stats.height;
  }

  public getStats(): BisqStats {
    logger.debug("getStats called from frontend");

    return this.stats;
  }

  public async $getBlocks(fromHeight: number, limit: number): Promise<[BisqBlock[], number]> {
    logger.debug(`getBlocks called from frontend; fromHeight=[${fromHeight}], limit=[${limit}]`);

    var cachedBlocks: BisqBlock[] = this.getRequiredBlocksFromCache(fromHeight, limit);
    if (cachedBlocks.length === limit) {
      return [cachedBlocks, this.stats.height - this.stats.genesisHeight];
    }

    var firstMissingBlockHeight = cachedBlocks.at(-1)?.height === undefined ? fromHeight : cachedBlocks.at(-1)?.height! + 1;
    var missingBlockCount = limit - cachedBlocks.length;

    await this.$fillMissingBlocksInCache(firstMissingBlockHeight, missingBlockCount);

    // now the cache should contain all the results needed
    cachedBlocks = this.getRequiredBlocksFromCache(fromHeight, limit);
    if (cachedBlocks.length !== limit) {
      logger.warn(`still missing blocks after cache fill; cache contains: ${cachedBlocks.length} / ${limit}`);
    }

    return [cachedBlocks, this.stats.height - this.stats.genesisHeight];
  }

  private async $pollForNewData() {
    this.lookupStats();

    if (this.isBisqAvailable() && new Date().getTime() - this.lastPollTimestamp > 60000) {
      this.lastPollTimestamp = new Date().getTime();
      this.pendingQueries.push(this.getCurrencies());
      this.pendingQueries.push(this.getOffers());
      this.pendingQueries.push(this.getTrades());
      Promise.allSettled(this.pendingQueries).then(() => {
        this.pendingQueries.length = 0;
        bisqMarket.updateCache();
      });
    }

    setTimeout(() => this.$pollForNewData(), 20000);
  }

  private isBisqAvailable(): boolean {
    if (this.stats.height > 0) {
      return true;
    }
    logger.warn("bisq not connected!");
    return false;
  }

  private async $fillMissingBlocksInCache(firstMissingBlockHeight: number, count: number) {
    logger.debug(`fill missing blocks in cache; firstMissingBlockHeight=[${firstMissingBlockHeight}] count=[${count}]`);

    for (let blockHeight = firstMissingBlockHeight; blockHeight < firstMissingBlockHeight + count; blockHeight++) {
      let block = this.blocks.find((b) => b.height === blockHeight);
      if (block === undefined) {
        logger.debug(`blockHeight [${blockHeight}] not found in cache, calling lookupBsqBlockByHeight`);
        const block = await this.$lookupBsqBlockByHeight(blockHeight);
        this.allBlocks.push(block);
      }
    }

    this.allBlocks = this.allBlocks.sort((a, b) => {
      return b['height'] >= a['height'] ? 1 : -1;
    });
    this.blocks = this.allBlocks;

    this.buildIndex();
  }

  private getRequiredBlocksFromCache(firstBlockHeight: number, count: number) {
    logger.debug(`get blocks from cache; firstBlockHeight=[${firstBlockHeight}], count=[${count}]`);

    const cachedBlocks: BisqBlock[] = [];
    for (let blockHeight = firstBlockHeight; blockHeight < firstBlockHeight + count; blockHeight++) {
      let block = this.blocks.find((b) => b.height === blockHeight);
      if (block === undefined) {
        // cache miss, force caller to index
        logger.debug(`returning incomplete results from cache lookup: ${cachedBlocks.length} / ${count}`);
        return cachedBlocks;
      } else {
        cachedBlocks.push(block);
      }
    }

    logger.debug(`found all ${cachedBlocks.length} blocks in cache`);
    return cachedBlocks;
  }

  private buildIndex() {
    logger.debug("start building index");

    this.allBlocks.forEach((block) => {
      if (!this.blockIndex[block.hash]) {
        this.blockIndex[block.hash] = block;
        logger.debug(`set block index for block hash [${block.hash}]`);
      }
    });

    logger.debug(`finished building index; blocks: ${this.blocks.length}, blockIndex: ${Object.keys(this.blockIndex).length}`);
  }

  private lookupStats() {
    const apiPromise = this.makeApiCall('dao/get-bsq-stats');
    apiPromise.then((buffer) => {
      try {
        const stats: BisqStats = JSON.parse(buffer)
        stats.minted /= 100.0;
        stats.burnt /= 100.0;
        stats._bsqPrice = bisqMarket.bsqPrice;
        stats._usdPrice = bisqMarket.bsqPrice * pricesUpdater.getLatestPrices()['USD'];
        stats._marketCap = stats._usdPrice * (stats.minted - stats.burnt);
        this.stats = stats;

        logger.debug(`stats: BSQ/BTC=${Bisq.FORMAT_BITCOIN(stats._bsqPrice)}, BSQ/USD=${Bisq.FORMAT_USD(stats._usdPrice)}, MktCap=${Bisq.FORMAT_USD(stats._marketCap)}, height=${stats.height}`);

        if (this.stats !== undefined && this.blocks.length < 30) {
          // startup, pre-cache first page or so of blocks
          this.$fillMissingBlocksInCache(this.stats.height, this.blocks.length + 10);
        } else if (this.stats !== undefined && this.blocks[0].height !== this.stats.height) {
          // cache a newly issued block
          this.$fillMissingBlocksInCache(this.stats.height, 1);
        }
      } catch (e) { Bisq.LOG_RESTAPI_DATA_ERR(e); }
    })
      .catch(err => { Bisq.LOG_RESTAPI_ERR(err) });
  }

  private async $lookupBsqTx(txId: string): Promise<BisqTransaction | undefined> {
    const apiPromise = this.makeApiCall('transactions/get-bsq-tx', [txId]);
    try {
      let buffer = await apiPromise;
      const tx: BisqTransaction = JSON.parse(buffer)
      return tx;
    } catch (e) { Bisq.LOG_RESTAPI_DATA_ERR(e); }
    return undefined;
  }

  private async $lookupBsqTxs(start: number, limit: number, types: string[]): Promise<BisqTransaction[]> {
    var joinedTypes = types.join("~")
    if (joinedTypes.length === 0) {
      joinedTypes = "~";
    }

    const apiPromise = this.makeApiCall('transactions/query-txs-paginated', [String(start), String(limit), joinedTypes]);
    try {
      let buffer = await apiPromise;
      const txs: BisqTransaction[] = JSON.parse(buffer)
      return txs;
    } catch (e) { Bisq.LOG_RESTAPI_DATA_ERR(e); }
    return [];
  }

  private async $lookupBsqTxForAddr(addr: string): Promise<BisqTransaction[]> {
    const apiPromise = this.makeApiCall('transactions/get-bsq-tx-for-addr', [addr]);
    try {
      let buffer = await apiPromise;
      const txs: BisqTransaction[] = JSON.parse(buffer)
      return txs;
    } catch (e) { Bisq.LOG_RESTAPI_DATA_ERR(e); }
    return [];
  }

  private async $lookupBsqBlockByHeight(height: number): Promise<BisqBlock> {
    const apiPromise = this.makeApiCall('blocks/get-bsq-block-by-height', [String(height)]);
    try {
      let buffer = await apiPromise;
      const block: BisqBlock = JSON.parse(buffer)
      return block;
    } catch (e) { Bisq.LOG_RESTAPI_DATA_ERR(e); }
    return {} as BisqBlock;
  }

  private async $lookupBsqBlockByHash(hash: string): Promise<BisqBlock | undefined> {
    const apiPromise = this.makeApiCall('blocks/get-bsq-block-by-hash', [hash]);
    try {
      let buffer = await apiPromise;
      const block: BisqBlock = JSON.parse(buffer)
      this.allBlocks.push(block);
      this.allBlocks = this.allBlocks.sort((a, b) => {
        return b['height'] >= a['height'] ? 1 : -1;
      });
      this.blocks = this.allBlocks;
      this.lookupStats();
      this.buildIndex();
      logger.debug(`blocks size is now ${this.blocks.length}`);
      return block;
    } catch (e) { Bisq.LOG_RESTAPI_DATA_ERR(e); }
  }

  private getCurrencies() {
    const apiPromise = this.makeApiCall('markets/get-currencies');
    apiPromise.then((buffer) => {
      try {
        bisqMarket.setCurrencyData(JSON.parse(buffer));
      } catch (e) { Bisq.LOG_RESTAPI_DATA_ERR(e); }
    })
      .catch(err => { Bisq.LOG_RESTAPI_ERR(err) });
    return apiPromise;
  }

  private getOffers() {
    const apiPromise = this.makeApiCall('markets/get-offers');
    apiPromise.then((buffer) => {
      try {
        bisqMarket.setOffersData(JSON.parse(buffer));
      } catch (e) { Bisq.LOG_RESTAPI_DATA_ERR(e); }
    })
      .catch(err => { Bisq.LOG_RESTAPI_ERR(err) });
    return apiPromise;
  }

  private getTrades() {
    const apiPromise = this.makeApiCall('markets/get-trades', [String(bisqMarket.getNewestTradeDate()), String(bisqMarket.getOldestTradeDate() - 1)]);
    apiPromise.then((buffer) => {
      try {
        bisqMarket.setTradesData(JSON.parse(buffer));
      } catch (e) { Bisq.LOG_RESTAPI_DATA_ERR(e); }
    })
      .catch(err => { Bisq.LOG_RESTAPI_ERR(err) });
    return apiPromise;
  }

  // requesting information from Bisq REST API process
  private makeApiCall(api_method: string, params?: string[]) {
    var pathStr = '/api/v1/explorer/' + api_method + '/';
    if (params !== undefined) {
      pathStr = pathStr + params.join("/");
    }
    logger.debug(`${pathStr}`);
    var requestOptions = {
      host: config.BISQ.HOST,
      port: config.BISQ.PORT,
      method: 'GET',
      path: pathStr,
      headers: {
        'Host': config.BISQ.HOST,
        'Content-Length': 0 //optPostRequest.length
      },
      agent: false,
      rejectUnauthorized: false
    }
    var request = http.request(requestOptions);
    //request.write(optPostRequest);
    request.end();
    var apiPromise = new Promise<string>((resolve, reject) => {
      request.on('error', function (e: any) {
        reject(new Error(`unable to make http request. ${JSON.stringify(requestOptions)}`));
      });
      request.on('response', (response: any) => {
        var buffer = ''
        response.on('data', function (chunk: any) {
          buffer = buffer + chunk
        })
        response.on('end', () => {
          resolve(buffer);
        });
      });
    });
    return apiPromise;
  }

  private static LOG_RESTAPI_ERR(err) {
    logger.err(`the Bisq daemon is not responding:\n${err}`);
  }

  private static LOG_RESTAPI_DATA_ERR(err) {
    logger.err(`{err}`);
  }

  private static FORMAT_BITCOIN(nbr): string {
    return nbr.toLocaleString('en-us', { maximumFractionDigits: 8 });
  }

  private static FORMAT_USD(nbr): string {
    return nbr.toLocaleString('en-us', { maximumFractionDigits: 2 });
  }
}

export default new Bisq();
