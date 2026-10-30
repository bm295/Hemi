import { CommonModule } from '@angular/common';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Component, inject, OnInit, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { firstValueFrom } from 'rxjs';

interface DiningTable { id: string; code: string; capacity: number; status: number }
interface MenuItem { id: string; name: string; category: string; price: number; isAvailable: boolean }
interface OrderLine { menuItemId: string; quantity: number; unitPrice: number; lineTotal: number }
interface ServiceOrder { id: string; tableId: string; createdAt: string; status: number; lines: OrderLine[]; totalAmount: number }
interface Reservation { id: string; guestName: string; partySize: number; reservedFor: string; contactPhone: string; notes: string | null; status: number }
interface InventoryItem { id: string; menuItemId: string; name: string; stockQuantity: number; unit: string }
interface SalesReport { totalRevenue: number; settledPayments: number; closedOrders: number }
type View = 'overview' | 'orders' | 'reservations' | 'inventory' | 'ml';
interface DemandResult { trainingCount: number; validationCount: number; testCount: number; selectedModel: string; possibleOverfitting: boolean; candidates: { name: string; training: { mae: number }; validation: { mae: number } }[]; test: { mae: number; rmse: number; rSquared: number } }

@Component({
  selector: 'app-root',
  imports: [CommonModule, FormsModule],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class App implements OnInit {
  private readonly http = inject(HttpClient);
  readonly view = signal<View>('overview');
  readonly loading = signal(true);
  readonly busy = signal(false);
  readonly error = signal('');
  readonly notice = signal('');
  readonly tables = signal<DiningTable[]>([]);
  readonly menu = signal<MenuItem[]>([]);
  readonly orders = signal<ServiceOrder[]>([]);
  readonly reservations = signal<Reservation[]>([]);
  readonly inventory = signal<InventoryItem[]>([]);
  readonly sales = signal<SalesReport | null>(null);
  readonly now = new Date();
  readonly mlResult = signal<any>(null);
  readonly mlPrediction = signal<any>(null);
  readonly mlBusy = signal(false);
  readonly mlError = signal('');
  readonly mlItemId = '11111111-1111-1111-1111-111111111111';
  mlDate = '2026-05-01';
  mlPrice = 120000;

  async trainMl(): Promise<void> {
    this.mlBusy.set(true);
    this.mlError.set('');
    try {
      const observations = Array.from({ length: 120 }, (_, i) => {
        const date = new Date(Date.UTC(2026, 0, 1 + i));
        const weekend = date.getUTCDay() === 0 || date.getUTCDay() === 6;
        return { date: date.toISOString().slice(0, 10), menuItemId: this.mlItemId,
          unitPrice: 120000, quantity: 12 + (weekend ? 9 : 0) + Math.floor(i / 20) + i % 5 };
      });
      this.mlResult.set(await firstValueFrom(this.http.post('/api/ml/demand/train',
        { menuItemId: this.mlItemId, observations })));
      this.mlPrediction.set(null);
    } catch (error) { this.mlError.set(this.message(error)); }
    finally { this.mlBusy.set(false); }
  }

  async predictMl(): Promise<void> {
    this.mlBusy.set(true);
    this.mlError.set('');
    try {
      this.mlPrediction.set(await firstValueFrom(this.http.post('/api/ml/demand/predict',
        { menuItemId: this.mlItemId, date: this.mlDate, unitPrice: this.mlPrice })));
    } catch (error) { this.mlError.set(this.message(error)); }
    finally { this.mlBusy.set(false); }
  }

  readonly demandResult = signal<DemandResult | null>(null);
  readonly demandPrediction = signal<number | null>(null);
  readonly demoItemId = '11111111-1111-1111-1111-111111111111';
  forecastDate = '2026-05-01';
  forecastPrice = 120000;

  async trainDemoDemand(): Promise<void> {
    this.busy.set(true); this.error.set('');
    try {
      const observations = Array.from({ length: 120 }, (_, i) => {
        const date = new Date(Date.UTC(2026, 0, 1 + i));
        const weekend = date.getUTCDay() === 0 || date.getUTCDay() === 6;
        return { date: date.toISOString().slice(0, 10), menuItemId: this.demoItemId, unitPrice: 120000, quantity: 12 + (weekend ? 9 : 0) + (i % 5) };
      });
      const result = await firstValueFrom(this.http.post<DemandResult>('/api/ml/demand/train', { menuItemId: this.demoItemId, observations }));
      this.demandResult.set(result); this.demandPrediction.set(null);
    } catch (error) { this.error.set(this.message(error)); }
    finally { this.busy.set(false); }
  }

  async predictDemoDemand(): Promise<void> {
    this.busy.set(true); this.error.set('');
    try {
      const result = await firstValueFrom(this.http.post<{ predictedQuantity: number }>('/api/ml/demand/predict', { menuItemId: this.demoItemId, date: this.forecastDate, unitPrice: this.forecastPrice }));
      this.demandPrediction.set(result.predictedQuantity);
    } catch (error) { this.error.set(this.message(error)); }
    finally { this.busy.set(false); }
  }

  newOrder = { tableId: '', menuItemId: '', quantity: 1 };
  newReservation = { guestName: '', partySize: 2, reservedFor: '', contactPhone: '', notes: '' };
  paymentMethods = [{ value: 0, label: 'Cash' }, { value: 1, label: 'Card' }, { value: 2, label: 'Bank transfer' }, { value: 3, label: 'E-wallet' }];
  selectedPaymentMethod = 0;

  async ngOnInit(): Promise<void> { await this.refresh(); }

  async refresh(): Promise<void> {
    this.loading.set(true);
    this.error.set('');
    try {
      const [tables, menu, orders, reservations, inventory, sales] = await Promise.all([
        firstValueFrom(this.http.get<DiningTable[]>('/api/tables')),
        firstValueFrom(this.http.get<MenuItem[]>('/api/menu')),
        firstValueFrom(this.http.get<ServiceOrder[]>('/api/orders/open')),
        firstValueFrom(this.http.get<Reservation[]>('/api/reservations/upcoming')),
        firstValueFrom(this.http.get<InventoryItem[]>('/api/inventory')),
        firstValueFrom(this.http.get<SalesReport>('/api/reports/sales')),
      ]);
      this.tables.set(tables);
      this.menu.set(menu);
      this.orders.set(orders);
      this.reservations.set(reservations);
      this.inventory.set(inventory);
      this.sales.set(sales);
    } catch (error) {
      this.error.set(this.message(error));
    } finally {
      this.loading.set(false);
    }
  }

  setView(view: View): void { this.view.set(view); this.error.set(''); this.notice.set(''); }
  tableCode(id: string): string { return this.tables().find(table => table.id === id)?.code ?? 'Unknown table'; }
  menuName(id: string): string { return this.menu().find(item => item.id === id)?.name ?? 'Menu item'; }
  orderStatus(status: number): string { return status === 1 ? 'In kitchen' : 'Open'; }
  reservationStatus(status: number): string { return ['Pending', 'Confirmed', 'Seated', 'Cancelled'][status] ?? 'Unknown'; }
  availableTables(): DiningTable[] { return this.tables().filter(table => table.status === 0); }
  lowStock(): InventoryItem[] { return this.inventory().filter(item => item.stockQuantity <= 10); }
  todayReservations(): Reservation[] {
    return this.reservations().filter(item => new Date(item.reservedFor).toDateString() === this.now.toDateString());
  }

  async createOrder(): Promise<void> {
    if (!this.newOrder.tableId || !this.newOrder.menuItemId || this.newOrder.quantity < 1) return;
    const created = await this.mutate(() => firstValueFrom(this.http.post('/api/orders', {
      tableId: this.newOrder.tableId,
      items: [{ menuItemId: this.newOrder.menuItemId, quantity: this.newOrder.quantity }],
    })), 'Order created');
    if (created) this.newOrder = { tableId: '', menuItemId: '', quantity: 1 };
  }

  async addItem(order: ServiceOrder, menuItemId: string): Promise<void> {
    if (!menuItemId) return;
    await this.mutate(() => firstValueFrom(this.http.post(`/api/orders/${order.id}/items`, { menuItemId, quantity: 1 })), 'Item added');
  }

  async sendToKitchen(order: ServiceOrder): Promise<void> {
    await this.mutate(() => firstValueFrom(this.http.post(`/api/orders/${order.id}/send-to-kitchen`, {})), 'Order sent to kitchen');
  }

  async payAndClose(order: ServiceOrder): Promise<void> {
    await this.mutate(async () => {
      await firstValueFrom(this.http.post(`/api/orders/${order.id}/payments`, { amount: order.totalAmount, method: Number(this.selectedPaymentMethod) }));
      await firstValueFrom(this.http.post(`/api/orders/${order.id}/close`, {}));
    }, 'Payment recorded and order closed');
  }

  async createReservation(): Promise<void> {
    if (!this.newReservation.guestName.trim() || !this.newReservation.contactPhone.trim() || !this.newReservation.reservedFor || this.newReservation.partySize < 1) return;
    const created = await this.mutate(() => firstValueFrom(this.http.post('/api/reservations', {
      ...this.newReservation,
      reservedFor: new Date(this.newReservation.reservedFor).toISOString(),
      notes: this.newReservation.notes || null,
    })), 'Reservation created');
    if (created) this.newReservation = { guestName: '', partySize: 2, reservedFor: '', contactPhone: '', notes: '' };
  }

  private async mutate(action: () => Promise<unknown>, success: string): Promise<boolean> {
    this.busy.set(true);
    this.error.set('');
    this.notice.set('');
    try {
      await action();
      this.notice.set(success);
      await this.refresh();
      return true;
    } catch (error) {
      this.error.set(this.message(error));
      return false;
    } finally {
      this.busy.set(false);
    }
  }

  private message(error: unknown): string {
    if (error instanceof HttpErrorResponse) {
      return error.error?.error ?? (error.status === 0 ? 'Cannot reach the API. Start the .NET server and check the proxy target.' : `Request failed (${error.status}).`);
    }
    return error instanceof Error ? error.message : 'Something went wrong.';
  }
}
