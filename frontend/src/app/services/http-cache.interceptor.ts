import { Inject, Injectable, PLATFORM_ID } from '@angular/core';
import { HttpInterceptor, HttpEvent, HttpRequest, HttpHandler, HttpResponse, HttpHeaders } from '@angular/common/http';
import { Observable, of } from 'rxjs';
import { tap } from 'rxjs/operators';
import { TransferState, makeStateKey } from '@angular/platform-browser';
import { isPlatformBrowser } from '@angular/common';

@Injectable()
export class HttpCacheInterceptor implements HttpInterceptor {
  isBrowser: boolean = isPlatformBrowser(this.platformId);

  constructor(
    private transferState: TransferState,
    @Inject(PLATFORM_ID) private platformId: any,
  ) { }

  intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    if (this.isBrowser && request.method === 'GET') {

      const { response, headers } = this.transferState.get<any>(makeStateKey(request.url), null) || {};
      if (response) {
        const httpHeaders = new HttpHeaders();
        for (const [k,v] of Object.entries(headers)) {
          httpHeaders.set(k,v as string[]);
        }
        const modifiedResponse = new HttpResponse<any>({
          headers: httpHeaders,
          body: response.body,
          status: response.status,
          statusText: response.statusText,
          url: response.url
        });
        this.transferState.remove(makeStateKey(request.url));
        return of(modifiedResponse);
      }
    }

    return next.handle(request)
      .pipe(tap((event: HttpEvent<any>) => {
        if (!this.isBrowser && event instanceof HttpResponse) {
          let keyId = request.url.split('/').slice(3).join('/');
          const headers = {};
          for (const k of event.headers.keys()) {
            headers[k] = event.headers.getAll(k);
          }
          this.transferState.set<any>(makeStateKey('/' + keyId), { response: event, headers });
        }
      }));
  }
}
